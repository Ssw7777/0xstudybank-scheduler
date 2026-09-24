const SITE = 'https://0xstudybank.vercel.app';
const RABBY = 'https://api.rabby.io/v1/user';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

export function selectShard(wallets, minute) {
  return wallets.filter((_, index) => index % 10 === minute % 10);
}

export async function jsonRequest(url, options = {}, timeout = 20000, runtime = {}) {
  const request = runtime.fetch ?? fetch;
  const wait = runtime.sleep ?? sleep;
  const isSite = new URL(url).origin === SITE;
  // Retry the SAME collected payload, never replace a failed read with zero.
  // 2+4+8+12+12+12 seconds covers the normal 45-second ingestion lease.
  const delays = isSite ? [2000, 4000, 8000, 12000, 12000, 12000] : [3000];
  for (let attempt = 0; ; attempt++) {
    let response;
    try {
      response = await request(url, { ...options, redirect: 'manual', signal: AbortSignal.timeout(timeout) });
    } catch (error) {
      if (attempt >= delays.length) throw error;
      await wait(delays[attempt]);
      continue;
    }
    const retryable = [408, 409, 429, 500, 502, 503, 504].includes(response.status);
    // The public provider has no guaranteed quota. Do not hammer the same
    // rate-limited host: abandon this shard and let the next minute retry later.
    if (!isSite && response.status === 429) {
      await response.body?.cancel().catch(() => {});
      throw new Error('upstream_rate_limited');
    }
    const body = response.ok ? await response.json() : null;
    const busy = isSite && response.status === 202 && body?.reason === 'refresh_in_progress';
    if ((retryable || busy) && attempt < delays.length) {
      const after = response.headers.get('retry-after');
      const retryMs = after === null ? 0 : (/^\d+(\.\d+)?$/.test(after) ? Number(after) * 1000 : Date.parse(after) - Date.now());
      await response.body?.cancel().catch(() => {});
      // Do not ignore long upstream cooldowns by retrying earlier than requested.
      if (retryMs > 60000) throw new Error('upstream_cooldown_exceeds_cycle');
      await wait(Math.max(delays[attempt], Number.isFinite(retryMs) ? retryMs : 0));
      continue;
    }
    if (!response.ok || busy) throw new Error(`upstream_http_${response.status}_${new URL(url).hostname}${new URL(url).pathname}`);
    return body;
  }
}

export async function runCycle(env, scheduledTime = Date.now(), manualSlot) {
  if (!env.CRON_SECRET) throw new Error('scheduler_secret_missing');
  const minute = Math.floor(scheduledTime / 60000);
  const slot = manualSlot ?? minute % 10;
  const headers = { Authorization: `Bearer ${env.CRON_SECRET}`, 'Content-Type': 'application/json' };
  const walletPolling = env.WALLET_POLLING_ENABLED !== 'false';
  if (!walletPolling && manualSlot !== undefined) throw new Error('wallet_polling_paused_provider_rate_limit');
  let wallets = [];
  if (walletPolling) {
    const manifest = await jsonRequest(`${SITE}/api/cron/wallet-targets`, { headers });
    if (!manifest.ok || !Array.isArray(manifest.wallets) || manifest.wallets.length > 50 || !manifest.wallets.length) throw new Error('invalid_manifest');
    const addresses = manifest.wallets.map(w => w.address);
    if (new Set(addresses).size !== addresses.length || addresses.some(a => !/^0x[a-f0-9]{40}$/.test(a))) throw new Error('invalid_addresses');
    wallets = selectShard(manifest.wallets, slot);
  }
  const observations = [];
  const execution = crypto.randomUUID();
  let failed = 0, protocolFailed = 0, deferred = 0;
  const failures = [];
  for (const wallet of wallets) {
    try {
      const observation = await jsonRequest(`${RABBY}/total_balance?id=${wallet.address}`);
      const total = Number(observation.total_usd_value ?? observation.total_usd);
      if (!Number.isFinite(total) || total < 0 || !Array.isArray(observation.chain_list)) throw new Error('invalid_balance');
      const entry = { address: wallet.address, observedAt: new Date().toISOString(), observation };
      // Total balance includes protocol value. Detailed positions are checked
      // each cycle too; failure must not erase the last successful positions.
      await sleep(4000);
      try {
        const protocols = await jsonRequest(`${RABBY}/complex_protocol_list?id=${wallet.address}`);
        const recognized = Array.isArray(protocols) || Array.isArray(protocols?.data) || Array.isArray(protocols?.list) || Array.isArray(protocols?.data?.list);
        if (!recognized) throw new Error('invalid_protocols');
        entry.protocolObservation = protocols;
      } catch (error) {
        protocolFailed++;
        failures.push({ wallet: wallet.id, stage: 'protocol', reason: String(error?.message ?? 'request_failed').slice(0, 180) });
        if (error?.message === 'upstream_rate_limited') {
          observations.push(entry);
          deferred = wallets.length - observations.length - failed;
          break;
        }
      }
      observations.push(entry);
    } catch (error) {
      failed++;
      failures.push({ wallet: wallet.id, stage: 'balance', reason: String(error?.message ?? 'request_failed').slice(0, 180) });
      if (error?.message === 'upstream_rate_limited') {
        deferred = wallets.length - observations.length - failed;
        break;
      }
    }
    await sleep(4000);
  }
  if (observations.length) {
    const accepted = await jsonRequest(`${SITE}/api/cron/wallet-observation`, {
      method: 'POST', headers: { ...headers, 'X-Idempotency-Key': `cf-${minute}-${slot}-${execution}` },
      body: JSON.stringify({ observations }),
    }, 30000);
    if (!accepted.ok || accepted.ran !== 'wallet-observations') throw new Error('observation_not_accepted');
  }
  let snapshot = 'not_due';
  if (minute % 5 === 0 && manualSlot === undefined) {
    const result = await jsonRequest(`${SITE}/api/cron/refresh?mode=fast`, {
      method: 'POST', headers: { ...headers, 'X-Idempotency-Key': `cf-fast-${minute}-${execution}` },
    }, 55000);
    snapshot = result.ok && result.reason !== 'refresh_in_progress' ? 'accepted' : 'not_completed';
  }
  const result = { ok: failed === 0 && protocolFailed === 0 && deferred === 0 && snapshot !== 'not_completed', walletPolling: walletPolling ? 'enabled' : 'paused_provider_rate_limit', slot, targeted: wallets.length, updated: observations.length, failed, protocolFailed, deferred, snapshot, failures };
  console.log(JSON.stringify(result));
  return result;
}

async function authorized(request, secret) {
  if (!secret) return false;
  const supplied = request.headers.get('authorization')?.replace(/^Bearer /, '') ?? '';
  const digest = value => crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  const [a, b] = await Promise.all([digest(supplied), digest(secret)]);
  const left = new Uint8Array(a), right = new Uint8Array(b);
  let difference = 0;
  for (let index = 0; index < left.length; index++) difference |= left[index] ^ right[index];
  return difference === 0;
}

export default {
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(runCycle(env, controller.scheduledTime).then(result => {
      if (!result.ok) throw new Error(`incomplete_refresh_slot_${result.slot}`);
    }));
  },
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname !== '/run' || request.method !== 'POST' || !(await authorized(request, env.CRON_SECRET))) return new Response('Not found', { status: 404 });
    const slot = Number(url.searchParams.get('slot'));
    if (!url.searchParams.has('slot') || !Number.isInteger(slot) || slot < 0 || slot > 9) return new Response('Invalid slot', { status: 400 });
    try { return Response.json(await runCycle(env, Date.now(), slot)); }
    catch (error) {
      const reason = error instanceof Error ? error.message.replaceAll(env.CRON_SECRET, '[redacted]').slice(0, 240) : 'network_or_runtime_error';
      console.log(JSON.stringify({ error: reason }));
      return Response.json({ ok: false, error: reason }, { status: 502 });
    }
  },
};
