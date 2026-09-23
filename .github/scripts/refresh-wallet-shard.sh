#!/usr/bin/env bash
set -Eeuo pipefail
set +x
umask 077

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

targets_file="$work_dir/wallet-targets.json"
observations_file="$work_dir/observations.ndjson"
batch_file="$work_dir/observation-batch.json"
response_file="$work_dir/observation-response.json"
mode="${MODE:-primary}"

if [[ "$mode" != "primary" && "$mode" != "retry" ]]; then
  echo "::error title=Scheduler configuration::MODE must be primary or retry."
  exit 1
fi
if [[ ! "${SHARD:-}" =~ ^[0-4]$ ]]; then
  echo "::error title=Scheduler configuration::SHARD must be an integer from 0 through 4."
  exit 1
fi

get_oidc_token() {
  local encoded_audience
  encoded_audience="$(node -e 'process.stdout.write(encodeURIComponent(process.argv[1]))' "$OIDC_AUDIENCE")"
  curl --fail --silent --show-error --connect-timeout 10 --max-time 20 \
    --retry 2 --retry-delay 2 --retry-all-errors \
    -H "Authorization: Bearer ${ACTIONS_ID_TOKEN_REQUEST_TOKEN}" \
    "${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=${encoded_audience}" \
    | node -e '
        let input = "";
        process.stdin.setEncoding("utf8");
        process.stdin.on("data", (chunk) => { input += chunk; });
        process.stdin.on("end", () => {
          const value = JSON.parse(input).value;
          if (typeof value !== "string" || !value) process.exit(1);
          process.stdout.write(value);
        });
      '
}

fetch_targets() {
  local token code
  token="$(get_oidc_token)"
  echo "::add-mask::$token"
  code="$(curl --silent --show-error --connect-timeout 10 --max-time 30 \
    --retry 2 --retry-delay 3 --retry-all-errors \
    -H "Authorization: Bearer ${token}" \
    -o "$targets_file" \
    -w '%{http_code}' \
    "$TARGETS_URL" || true)"
  if [[ ! "$code" =~ ^2[0-9][0-9]$ ]]; then
    echo "::error title=Wallet manifest::Target endpoint returned HTTP ${code:-000}."
    return 1
  fi

  TARGETS_FILE="$targets_file" node <<'NODE'
const fs = require("fs");
const body = JSON.parse(fs.readFileSync(process.env.TARGETS_FILE, "utf8"));
if (body.ok !== true || !Array.isArray(body.wallets)) process.exit(1);
// Five shards may each process at most five addresses in a cycle.
if (body.wallets.length !== 25) {
  console.log(`::error title=Wallet manifest::Expected 25 addresses, received ${body.wallets.length}.`);
  process.exit(1);
}
const addresses = body.wallets.map((wallet) => String(wallet && wallet.address || "").toLowerCase());
if (new Set(addresses).size !== 25) {
  console.log("::error title=Wallet manifest::The manifest contains duplicate addresses.");
  process.exit(1);
}
for (const wallet of body.wallets) {
  if (!wallet || typeof wallet.address !== "string" || !/^0x[a-fA-F0-9]{40}$/.test(wallet.address)) {
    process.exit(1);
  }
}
console.log(`Wallet manifest: ${body.wallets.length} configured addresses.`);
NODE
}

target_field() {
  local index="$1"
  local field="$2"
  TARGETS_FILE="$targets_file" INDEX="$index" FIELD="$field" node -e '
    const fs = require("fs");
    const data = JSON.parse(fs.readFileSync(process.env.TARGETS_FILE, "utf8"));
    const wallet = data.wallets[Number(process.env.INDEX)];
    if (!wallet || !/^0x[a-fA-F0-9]{40}$/.test(wallet.address)) process.exit(1);
    if (process.env.FIELD === "address") process.stdout.write(wallet.address.toLowerCase());
    else if (process.env.FIELD === "needsProtocolRefresh") process.stdout.write(wallet.needsProtocolRefresh === true ? "true" : "false");
    else process.stdout.write(wallet.needsRetry === true ? "true" : "false");
  '
}

collect_observation() {
  local index="$1"
  local ordinal="$2"
  local address rabby_file protocol_file code protocol_code observed_at
  address="$(target_field "$index" address)"
  rabby_file="$work_dir/rabby-${ordinal}.json"
  protocol_file="$work_dir/protocol-${ordinal}.json"

  code="$(curl --silent --show-error --connect-timeout 10 --max-time 25 \
    --retry 1 --retry-delay 4 --retry-all-errors --fail-with-body \
    -H "Accept: application/json" \
    -H "User-Agent: 0xstudybank-scheduler/2.0" \
    -o "$rabby_file" \
    -w '%{http_code}' \
    "${RABBY_TOTAL_URL}?id=${address}" || true)"
  if [[ ! "$code" =~ ^2[0-9][0-9]$ ]]; then
    echo "::warning title=wallet-${ordinal}::Rabby returned HTTP ${code:-000}; the retry stage will use a fresh runner."
    return 1
  fi

  observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ "$(target_field "$index" needsProtocolRefresh)" == "true" ]]; then
    sleep "$RABBY_CALL_INTERVAL_SECONDS"
    protocol_code="$(curl --silent --show-error --connect-timeout 10 --max-time 25 \
      --retry 1 --retry-delay 4 --retry-all-errors --fail-with-body \
      -H "Accept: application/json" \
      -H "User-Agent: 0xstudybank-scheduler/2.0" \
      -o "$protocol_file" \
      -w '%{http_code}' \
      "${RABBY_PROTOCOL_URL}?id=${address}" || true)"
    if [[ ! "$protocol_code" =~ ^2[0-9][0-9]$ ]]; then
      rm -f "$protocol_file"
      echo "::warning title=wallet-${ordinal}-protocols::Rabby protocol endpoint returned HTTP ${protocol_code:-000}; cached protocol data is retained."
    fi
  fi

  ADDRESS="$address" OBSERVED_AT="$observed_at" RABBY_FILE="$rabby_file" PROTOCOL_FILE="$protocol_file" OBSERVATIONS_FILE="$observations_file" node <<'NODE'
const fs = require("fs");
const observation = JSON.parse(fs.readFileSync(process.env.RABBY_FILE, "utf8"));
const total = Number(observation && (observation.total_usd_value ?? observation.total_usd));
if (!Number.isFinite(total) || total < 0 || !Array.isArray(observation.chain_list)) process.exit(1);
const entry = {
  address: process.env.ADDRESS,
  observedAt: process.env.OBSERVED_AT,
  observation,
};
if (process.env.PROTOCOL_FILE && fs.existsSync(process.env.PROTOCOL_FILE)) {
  try {
    const protocolObservation = JSON.parse(fs.readFileSync(process.env.PROTOCOL_FILE, "utf8"));
    const recognized = Array.isArray(protocolObservation) ||
      Array.isArray(protocolObservation && protocolObservation.data) ||
      Array.isArray(protocolObservation && protocolObservation.list) ||
      Array.isArray(protocolObservation && protocolObservation.data && protocolObservation.data.list);
    if (!recognized) throw new Error("invalid protocol payload");
    entry.protocolObservation = protocolObservation;
  } catch {
    console.log(`::warning title=wallet-protocols::Invalid protocol payload for wallet ${process.env.ADDRESS.slice(0, 6)}…; cached data is retained.`);
  }
}
fs.appendFileSync(process.env.OBSERVATIONS_FILE, `${JSON.stringify(entry)}\n`);
NODE
}

build_batch() {
  OBSERVATIONS_FILE="$observations_file" BATCH_FILE="$batch_file" node <<'NODE'
const fs = require("fs");
const text = fs.existsSync(process.env.OBSERVATIONS_FILE)
  ? fs.readFileSync(process.env.OBSERVATIONS_FILE, "utf8").trim()
  : "";
const observations = text ? text.split(/\r?\n/).map((line) => JSON.parse(line)) : [];
if (observations.length > 5) process.exit(1);
fs.writeFileSync(process.env.BATCH_FILE, JSON.stringify({ observations }));
process.stdout.write(String(observations.length));
NODE
}

post_batch() {
  local observation_count="$1"
  local attempt token code delay

  # Shards normally finish together. Deterministic spacing plus exponential
  # retry keeps optimistic store writes from colliding.
  sleep "$((SHARD * BATCH_STAGGER_SECONDS))"
  for attempt in 1 2 3 4; do
    token="$(get_oidc_token)"
    echo "::add-mask::$token"
    code="$(curl --silent --show-error --connect-timeout 10 --max-time 35 \
      -X POST \
      -H "Authorization: Bearer ${token}" \
      -H "X-Idempotency-Key: ${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-${mode}-shard-${SHARD}" \
      -H "Content-Type: application/json" \
      --data-binary "@$batch_file" \
      -o "$response_file" \
      -w '%{http_code}' \
      "$OBSERVATION_URL" || true)"
    if [[ "$code" =~ ^2[0-9][0-9]$ ]] && \
      RESPONSE_FILE="$response_file" node -e '
        const fs = require("fs");
        const body = JSON.parse(fs.readFileSync(process.env.RESPONSE_FILE, "utf8"));
        if (body.ok !== true || body.ran !== "wallet-observations") process.exit(1);
      '; then
      echo "${mode} shard ${SHARD}: batch accepted (${observation_count} observations)."
      return 0
    fi

    if (( attempt < 4 )); then
      delay=$((4 * (2 ** (attempt - 1))))
      echo "::warning title=${mode}-shard-${SHARD}::Batch write returned HTTP ${code:-000}; retrying in ${delay}s."
      sleep "$delay"
    fi
  done

  echo "::warning title=${mode}-shard-${SHARD}::Batch write failed after four attempts; final freshness verification will decide the cycle."
  return 1
}

fetch_targets

failed_wallets=0
candidates=0
position=0
for index in $(seq "$SHARD" "$SHARD_COUNT" 24); do
  if [[ "$mode" == "retry" ]] && [[ "$(target_field "$index" needsRetry)" != "true" ]]; then
    continue
  fi

  if (( position > 0 )); then sleep "$RABBY_CALL_INTERVAL_SECONDS"; fi
  candidates=$((candidates + 1))
  ordinal=$((index + 1))
  if ! collect_observation "$index" "$ordinal"; then
    failed_wallets=$((failed_wallets + 1))
  fi
  position=$((position + 1))
done

observation_count="$(build_batch)"
batch_failed=0
if (( observation_count > 0 )); then
  if ! post_batch "$observation_count"; then batch_failed=1; fi
else
  echo "${mode} shard ${SHARD}: no observations to write."
fi

if (( failed_wallets > 0 )); then
  echo "::warning title=Wallet ${mode}::${failed_wallets}/${candidates} Rabby observation(s) failed in shard ${SHARD}."
fi
if (( batch_failed > 0 )); then
  echo "::warning title=Wallet ${mode}::The collected observations were not persisted in shard ${SHARD}."
fi
echo "${mode} shard ${SHARD} completed; candidates=${candidates}, collected=${observation_count}, readFailures=${failed_wallets}, writeFailures=${batch_failed}."
