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
rate_limited=0
collection_started=$SECONDS
collection_budget_seconds=240

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
    else if (process.env.FIELD === "needsTokenRefresh") process.stdout.write(wallet.needsTokenRefresh === true ? "true" : "false");
    else process.stdout.write(wallet.needsRetry === true ? "true" : "false");
  '
}

collect_observation() {
  local index="$1"
  local ordinal="$2"
  local address rabby_file protocol_file token_observations_file code protocol_code observed_at
  local wallet_started token_chain token_file token_code token_observed_at token_timeout remaining_wallet remaining_shard
  local token_specific_observations_file specific_request specific_file specific_code specific_observed_at specific_ordinal
  wallet_started=$SECONDS
  address="$(target_field "$index" address)"
  rabby_file="$work_dir/rabby-${ordinal}.json"
  protocol_file="$work_dir/protocol-${ordinal}.json"
  token_observations_file="$work_dir/tokens-${ordinal}.ndjson"
  token_specific_observations_file="$work_dir/specific-tokens-${ordinal}.ndjson"

  code="$(curl --silent --show-error --connect-timeout 10 --max-time 25 \
    --fail-with-body \
    -H "Accept: application/json" \
    -H "User-Agent: 0xstudybank-scheduler/2.0" \
    -o "$rabby_file" \
    -w '%{http_code}' \
    "${RABBY_TOTAL_URL}?id=${address}" || true)"
  if [[ ! "$code" =~ ^2[0-9][0-9]$ ]]; then
    if [[ "$code" == "429" ]]; then rate_limited=1; fi
    echo "::warning title=wallet-${ordinal}::Rabby returned HTTP ${code:-000}; on rate limiting this shard stops and the retry stage is skipped."
    return 1
  fi

  observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ "$(target_field "$index" needsProtocolRefresh)" == "true" ]]; then
    sleep "$RABBY_CALL_INTERVAL_SECONDS"
    protocol_code="$(curl --silent --show-error --connect-timeout 10 --max-time 25 \
      --fail-with-body \
      -H "Accept: application/json" \
      -H "User-Agent: 0xstudybank-scheduler/2.0" \
      -o "$protocol_file" \
      -w '%{http_code}' \
      "${RABBY_PROTOCOL_URL}?id=${address}" || true)"
    if [[ ! "$protocol_code" =~ ^2[0-9][0-9]$ ]]; then
      if [[ "$protocol_code" == "429" ]]; then rate_limited=1; fi
      rm -f "$protocol_file"
      echo "::warning title=wallet-${ordinal}-protocols::Rabby protocol endpoint returned HTTP ${protocol_code:-000}; cached protocol data is retained."
    fi
  fi

  # Query an explicit chain_id: all-chain coverage was not established by the
  # live probe. An HTTP 200 empty response without it must not clear a wallet.
  # First refresh known token UUIDs across chains in bounded batches, then discover
  # new holdings on one chain. A missing specific-token result is not a zero.
  if (( rate_limited == 0 )) && { [[ "$mode" == "primary" ]] || [[ "$(target_field "$index" needsTokenRefresh)" == "true" ]]; }; then
    specific_ordinal=0
    while IFS= read -r specific_request; do
      [[ -n "$specific_request" ]] || continue
      remaining_wallet=$((100 - (SECONDS - wallet_started)))
      remaining_shard=$((collection_budget_seconds - (SECONDS - collection_started)))
      if (( remaining_wallet <= RABBY_CALL_INTERVAL_SECONDS + 2 || remaining_shard <= RABBY_CALL_INTERVAL_SECONDS + 2 )); then
        echo "::warning title=wallet-${ordinal}-tokens::Known-token collection budget reached; unqueried tokens retain their old audit timestamps."
        break
      fi
      sleep "$RABBY_CALL_INTERVAL_SECONDS"
      token_timeout=$((100 - (SECONDS - wallet_started)))
      remaining_shard=$((collection_budget_seconds - (SECONDS - collection_started)))
      if (( token_timeout > remaining_shard )); then token_timeout=$remaining_shard; fi
      if (( token_timeout > 25 )); then token_timeout=25; fi
      specific_ordinal=$((specific_ordinal + 1))
      specific_file="$work_dir/specific-${ordinal}-${specific_ordinal}.json"
      specific_code="$(curl --silent --show-error --connect-timeout 10 --max-time "$token_timeout" \
        --fail-with-body \
        -X POST \
        -H "Accept: application/json" \
        -H "Content-Type: application/json" \
        -H "User-Agent: 0xstudybank-scheduler/2.0" \
        --data-binary "$specific_request" \
        -o "$specific_file" \
        -w '%{http_code}' \
        "$RABBY_SPECIFIC_TOKEN_URL" || true)"
      specific_observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      SPECIFIC_REQUEST="$specific_request" SPECIFIC_FILE="$specific_file" SPECIFIC_CODE="${specific_code:-000}" SPECIFIC_OBSERVED_AT="$specific_observed_at" TOKEN_SPECIFIC_OBSERVATIONS_FILE="$token_specific_observations_file" node <<'NODE'
const fs = require("fs");
const requestedUuids = JSON.parse(process.env.SPECIFIC_REQUEST).uuids;
const entry = { requestedUuids, observedAt: process.env.SPECIFIC_OBSERVED_AT };
const code = process.env.SPECIFIC_CODE;
if (/^2\d\d$/.test(code)) {
  try {
    const body = JSON.parse(fs.readFileSync(process.env.SPECIFIC_FILE, "utf8"));
    if (!Array.isArray(body)) throw new Error("invalid specific token list");
    entry.observation = body;
  } catch {
    entry.tokenError = "invalid_payload";
  }
} else {
  entry.tokenError = `http_${code}`;
}
if (entry.tokenError) console.log(`::warning title=Known-token audit::${entry.tokenError}; previous quantities are retained.`);
fs.appendFileSync(process.env.TOKEN_SPECIFIC_OBSERVATIONS_FILE, `${JSON.stringify(entry)}\n`);
NODE
      if [[ "$specific_code" == "429" ]]; then rate_limited=1; break; fi
    done < <(TARGETS_FILE="$targets_file" INDEX="$index" ADDRESS="$address" node <<'NODE'
const fs = require("fs");
const wallet = JSON.parse(fs.readFileSync(process.env.TARGETS_FILE, "utf8")).wallets[Number(process.env.INDEX)];
const uuids = [...new Set((Array.isArray(wallet.tokenUuids) ? wallet.tokenUuids : []).filter(value =>
  typeof value === "string" && /^[a-zA-Z0-9_-]{1,48}:[a-zA-Z0-9_.-]{1,160}$/.test(value)
))];
for (let offset = 0; offset < Math.min(uuids.length, 300); offset += 100) {
  console.log(JSON.stringify({ id: process.env.ADDRESS, uuids: uuids.slice(offset, offset + 100) }));
}
NODE
    )

    # tokenChains is ordered least recently discovered first by the server.
    # Querying only one chain per round avoids an unbounded chain-count multiplier.
    if (( rate_limited == 0 )); then
    while IFS= read -r token_chain; do
      [[ -n "$token_chain" ]] || continue
      remaining_wallet=$((100 - (SECONDS - wallet_started)))
      remaining_shard=$((collection_budget_seconds - (SECONDS - collection_started)))
      if (( remaining_wallet <= RABBY_CALL_INTERVAL_SECONDS + 2 || remaining_shard <= RABBY_CALL_INTERVAL_SECONDS + 2 )); then
        echo "::warning title=wallet-${ordinal}-tokens::Token collection budget reached; unqueried chains keep their previous audit timestamps."
        break
      fi
      sleep "$RABBY_CALL_INTERVAL_SECONDS"
      token_timeout=$((100 - (SECONDS - wallet_started)))
      remaining_shard=$((collection_budget_seconds - (SECONDS - collection_started)))
      if (( token_timeout > remaining_shard )); then token_timeout=$remaining_shard; fi
      if (( token_timeout > 25 )); then token_timeout=25; fi
      token_file="$work_dir/token-${ordinal}-${token_chain}.json"
      token_code="$(curl --silent --show-error --connect-timeout 10 --max-time "$token_timeout" \
        --fail-with-body \
        -H "Accept: application/json" \
        -H "User-Agent: 0xstudybank-scheduler/2.0" \
        -o "$token_file" \
        -w '%{http_code}' \
        "${RABBY_TOKEN_URL}?id=${address}&chain_id=${token_chain}&is_all=true" || true)"
      token_observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      TOKEN_CHAIN="$token_chain" TOKEN_FILE="$token_file" TOKEN_CODE="${token_code:-000}" TOKEN_OBSERVED_AT="$token_observed_at" TOKEN_OBSERVATIONS_FILE="$token_observations_file" node <<'NODE'
const fs = require("fs");
const entry = { chain: process.env.TOKEN_CHAIN, observedAt: process.env.TOKEN_OBSERVED_AT };
const code = process.env.TOKEN_CODE;
if (/^2\d\d$/.test(code)) {
  try {
    const body = JSON.parse(fs.readFileSync(process.env.TOKEN_FILE, "utf8"));
    if (!Array.isArray(body)) throw new Error("invalid token list");
    entry.observation = body;
  } catch {
    entry.tokenError = "invalid_payload";
  }
} else {
  entry.tokenError = `http_${code}`;
}
if (entry.tokenError) console.log(`::warning title=Token audit::Chain ${entry.chain}: ${entry.tokenError}; previous quantities are retained.`);
fs.appendFileSync(process.env.TOKEN_OBSERVATIONS_FILE, `${JSON.stringify(entry)}\n`);
NODE
      if [[ "$token_code" == "429" ]]; then rate_limited=1; break; fi
    done < <(TARGETS_FILE="$targets_file" INDEX="$index" RABBY_FILE="$rabby_file" node <<'NODE'
const fs = require("fs");
const wallet = JSON.parse(fs.readFileSync(process.env.TARGETS_FILE, "utf8")).wallets[Number(process.env.INDEX)];
const balance = JSON.parse(fs.readFileSync(process.env.RABBY_FILE, "utf8"));
const validChain = (value) => typeof value === "string" && /^[a-zA-Z0-9_-]{1,48}$/.test(value);
const planned = Array.isArray(wallet.tokenChains) ? wallet.tokenChains.filter(validChain) : [];
const observed = Array.isArray(balance.chain_list)
  ? balance.chain_list.filter((chain) => Number(chain.usd_value ?? chain.total_usd_value ?? 0) > 0).map((chain) => chain.id).filter(validChain)
  : [];
const chains = [...new Set([...planned, ...observed])].slice(0, 1);
for (const chain of chains) console.log(chain);
NODE
    )
    fi
  fi

  ADDRESS="$address" OBSERVED_AT="$observed_at" RABBY_FILE="$rabby_file" PROTOCOL_FILE="$protocol_file" TOKEN_OBSERVATIONS_FILE="$token_observations_file" TOKEN_SPECIFIC_OBSERVATIONS_FILE="$token_specific_observations_file" OBSERVATIONS_FILE="$observations_file" node <<'NODE'
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
if (fs.existsSync(process.env.TOKEN_OBSERVATIONS_FILE)) {
  const tokens = fs.readFileSync(process.env.TOKEN_OBSERVATIONS_FILE, "utf8").trim();
  if (tokens) entry.tokenObservations = tokens.split(/\r?\n/).map((line) => JSON.parse(line));
}
if (fs.existsSync(process.env.TOKEN_SPECIFIC_OBSERVATIONS_FILE)) {
  const tokens = fs.readFileSync(process.env.TOKEN_SPECIFIC_OBSERVATIONS_FILE, "utf8").trim();
  if (tokens) entry.tokenSpecificObservations = tokens.split(/\r?\n/).map((line) => JSON.parse(line));
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
  # Token staleness alone must not re-read an otherwise fresh wallet balance.
  # Its ordered chain queue continues during the next primary round.
  if [[ "$mode" == "retry" ]] && [[ "$(target_field "$index" needsRetry)" != "true" ]]; then
    continue
  fi

  if (( SECONDS - collection_started >= collection_budget_seconds )); then
    echo "::warning title=Wallet collection::Shard collection budget reached; submitting completed observations before the runner timeout."
    break
  fi

  if (( position > 0 )); then sleep "$RABBY_CALL_INTERVAL_SECONDS"; fi
  candidates=$((candidates + 1))
  ordinal=$((index + 1))
  if ! collect_observation "$index" "$ordinal"; then
    failed_wallets=$((failed_wallets + 1))
  fi
  position=$((position + 1))
  if (( rate_limited > 0 )); then break; fi
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
if (( rate_limited > 0 )); then
  echo "::error title=Provider rate limit::HTTP 429: stopping further requests; no fresh-runner retry is allowed."
  exit 75
fi
