import { pathToFileURL } from 'node:url';

const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const timestamp = value => Date.parse(value ?? '') || 0;
export function oldestFirst(wallets, field, rotation = 0) {
  const offset = rotation % wallets.length;
  return [...wallets.slice(offset), ...wallets.slice(0, offset)]
    .sort((a, b) => timestamp(a[field]) - timestamp(b[field]));
}

export function validateManifest(body) {
  if (body?.ok !== true || !Array.isArray(body.wallets) || !body.wallets.length || body.wallets.length > 50)
    throw new Error('invalid_manifest');
  const addresses = body.wallets.map(w => String(w?.address ?? '').toLowerCase());
  if (new Set(addresses).size !== addresses.length || addresses.some(a => !/^0x[a-f0-9]{40}$/.test(a)))
    throw new Error('invalid_addresses');
  return body.wallets.map((w, i) => ({ ...w, address: addresses[i] }));
}

// Dependency injection makes starvation, partial persistence and 429 handling
// testable without spending provider quota or writing production observations.
export async function collectCycle(wallets, io, options = {}) {
  const now = io.now ?? Date.now;
  const start = now();
  const budgetMs = options.budgetMs ?? 480_000;
  const rotation = options.rotation ?? 0;
  const totals = new Map();
  const report = { configured: wallets.length, balances: 0, details: 0, readFailures: 0, rateLimited: false, budgetReached: false };
  let pending = [];
  async function flush() {
    if (!pending.length) return;
    await io.persist(pending);
    pending = [];
  }
  async function save(entry) {
    // The ingestion API rejects duplicate addresses in one atomic batch.
    if (pending.some(item => item.address === entry.address)) await flush();
    pending.push(entry);
    if (pending.length >= 5) await flush();
  }
  function canRead() {
    if (now() - start >= budgetMs) report.budgetReached = true;
    return !report.rateLimited && !report.budgetReached;
  }
  async function read(stage, wallet, extra) {
    try { return await io.read(stage, wallet, extra); }
    catch (error) {
      report.readFailures++;
      if (error?.status === 429) report.rateLimited = true;
      io.log?.(`${stage}: ${error?.status ? `HTTP ${error.status}` : 'request failed'}; previous successful data retained.`);
      return { error: error?.status ? `http_${error.status}` : 'request_failed' };
    }
  }

  // Phase 1 MUST complete before any detail request. Persist every five wallets,
  // and also persist a partial batch on rate limit or budget exhaustion.
  for (const wallet of oldestFirst(wallets, 'lastSuccessAt', rotation)) {
    if (!canRead()) break;
    const result = await read('balance', wallet);
    if (result.error) continue;
    const raw = result.body;
    const total = raw?.total_usd_value ?? raw?.total_usd;
    if (total == null || !Number.isFinite(Number(total)) || Number(total) < 0 || !Array.isArray(raw?.chain_list)) {
      report.readFailures++;
      continue;
    }
    const entry = { address: wallet.address, observedAt: new Date(now()).toISOString(), observation: raw };
    totals.set(wallet.address, entry);
    await save(entry);
    report.balances++;
  }
  await flush();
  io.log?.(`Wallet totals persisted: ${report.balances}/${wallets.length}.`);

  // Phase 2 refreshes known quantities across chains before expensive discovery
  // or protocol endpoints. Failed specific-token requests are recorded, not zeroed.
  const available = wallets.filter(w => totals.has(w.address));
  for (const wallet of oldestFirst(available, 'tokenAttemptAt', rotation)) {
    const uuids = [...new Set((wallet.tokenUuids ?? []).filter(id => typeof id === 'string' && /^[a-z0-9_-]{1,40}:(0x[a-f0-9]{40}|[a-z0-9_-]{1,40})$/.test(id)))];
    for (let offset = 0; offset < Math.min(uuids.length, 300); offset += 100) {
      if (!canRead()) break;
      const requestedUuids = uuids.slice(offset, offset + 100);
      const result = await read('tokens', wallet, requestedUuids);
      const detail = { requestedUuids, observedAt: new Date(now()).toISOString() };
      if (result.error || !Array.isArray(result.body)) detail.tokenError = result.error ?? 'invalid_payload';
      else detail.observation = result.body;
      await save({ ...totals.get(wallet.address), tokenSpecificObservations: [detail] });
      if (!detail.tokenError) report.details++;
    }
    if (!canRead()) break;
  }
  await flush();

  // Interleave protocol and new-asset discovery work. A permanently due protocol
  // backlog must not consume every remaining slot and starve discovery forever.
  const protocols = oldestFirst(available, 'protocolUpdatedAt', rotation).filter(w => w.needsProtocolRefresh === true);
  const discovery = oldestFirst(available, 'tokenDiscoveryAt', rotation);
  const tasks = [];
  for (let i = 0; i < Math.max(protocols.length, discovery.length); i++) {
    if (protocols[i]) tasks.push({ stage: 'protocols', wallet: protocols[i] });
    if (discovery[i]) tasks.push({ stage: 'discovery', wallet: discovery[i] });
  }
  for (const { stage, wallet } of tasks) {
    if (!canRead()) break;
    if (stage === 'protocols') {
      const result = await read('protocols', wallet);
      const body = result.body;
      if (!result.error && (Array.isArray(body) || Array.isArray(body?.data) || Array.isArray(body?.list) || Array.isArray(body?.data?.list))) {
        await save({ ...totals.get(wallet.address), protocolObservation: body });
        report.details++;
      }
      continue;
    }
    const observed = totals.get(wallet.address).observation.chain_list
      .filter(c => Number(c.usd_value ?? c.total_usd_value ?? 0) > 0).map(c => c.id);
    const chain = [...(wallet.tokenChains ?? []), ...observed].find(c => typeof c === 'string' && /^[a-z0-9_-]{1,40}$/.test(c));
    if (!chain) continue;
    const result = await read('discovery', wallet, chain);
    const detail = { chain, observedAt: new Date(now()).toISOString() };
    if (result.error || !Array.isArray(result.body)) detail.tokenError = result.error ?? 'invalid_payload';
    else detail.observation = result.body;
    await save({ ...totals.get(wallet.address), tokenObservations: [detail] });
    if (!detail.tokenError) report.details++;
  }
  await flush();
  return report;
}

async function main() {
  const env = process.env;
  let auth, authAt = 0, sequence = 0, nextProviderAt = 0;
  async function authorization() {
    if (auth && Date.now() - authAt < 180_000) return auth;
    const url = new URL(env.ACTIONS_ID_TOKEN_REQUEST_URL);
    url.searchParams.set('audience', env.OIDC_AUDIENCE);
    const response = await fetch(url, { headers: { Authorization: `Bearer ${env.ACTIONS_ID_TOKEN_REQUEST_TOKEN}` }, signal: AbortSignal.timeout(20_000), redirect: 'error' });
    if (!response.ok) throw new Error(`oidc_http_${response.status}`);
    auth = (await response.json()).value;
    if (!auth) throw new Error('oidc_missing');
    authAt = Date.now();
    console.log(`::add-mask::${auth}`);
    return auth;
  }
  async function site(url, body, key) {
    const delays = [2000, 4000, 8000, 12000, 12000, 12000];
    for (let attempt = 0; ; attempt++) {
      let response, data;
      try {
        response = await fetch(url, {
          method: body ? 'POST' : 'GET', redirect: 'error', signal: AbortSignal.timeout(35_000),
          headers: { Authorization: `Bearer ${await authorization()}`, 'Content-Type': 'application/json', ...(key ? { 'X-Idempotency-Key': key } : {}) },
          ...(body ? { body: JSON.stringify(body) } : {}),
        });
        data = await response.json();
      } catch { /* Retry a lost response with the same observation and key. */ }
      if (response?.ok && data?.ok === true && (!body || data.ran === 'wallet-observations')) return data;
      if (attempt >= delays.length || (response && response.status >= 400 && response.status < 500 && ![408,409,429].includes(response.status)))
        throw new Error(`site_http_${response?.status ?? 'network'}_not_accepted`);
      await pause(delays[attempt]);
    }
  }
  const wallets = validateManifest(await site(env.TARGETS_URL));
  const endpoints = { balance: env.RABBY_TOTAL_URL, tokens: env.RABBY_SPECIFIC_TOKEN_URL, protocols: env.RABBY_PROTOCOL_URL, discovery: env.RABBY_TOKEN_URL };
  const report = await collectCycle(wallets, {
    log: text => console.log(text),
    persist: async observations => {
      await site(env.OBSERVATION_URL, { observations }, `${env.GITHUB_RUN_ID}-${env.GITHUB_RUN_ATTEMPT}-cycle-${sequence++}`);
      console.log(`Persisted ${observations.length} observations.`);
    },
    read: async (stage, wallet, extra) => {
      await pause(Math.max(0, nextProviderAt - Date.now()));
      try {
        const url = new URL(endpoints[stage]);
        if (stage !== 'tokens') url.searchParams.set('id', wallet.address);
        if (stage === 'discovery') { url.searchParams.set('chain_id', extra); url.searchParams.set('is_all', 'true'); }
        const response = await fetch(url, {
          method: stage === 'tokens' ? 'POST' : 'GET', redirect: 'error', signal: AbortSignal.timeout(25_000),
          headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'User-Agent': '0xstudybank-scheduler/3.0' },
          ...(stage === 'tokens' ? { body: JSON.stringify({ id: wallet.address, uuids: extra }) } : {}),
        });
        if (!response.ok) { await response.body?.cancel(); throw Object.assign(new Error('provider_request_failed'), { status: response.status }); }
        return { body: await response.json() };
      // Production returned 429 on request eleven within one minute at 5s.
      // Eight seconds between completions reduces pressure below that observed
      // ceiling; it is not a claim that the public API guarantees this quota.
      } finally { nextProviderAt = Date.now() + 8000; }
    },
  }, { rotation: Number(env.GITHUB_RUN_NUMBER ?? 0) });
  console.log(JSON.stringify(report));
  if (report.rateLimited) { console.log('::error::Provider HTTP 429; stopped all further provider requests. No runner retry.'); process.exitCode = 75; }
  else if (report.balances !== wallets.length) { console.log('::error::Incomplete wallet totals; oldest wallets remain first next cycle.'); process.exitCode = 1; }
  else if (report.budgetReached || report.readFailures) console.log('::warning::Totals persisted; detail audit is incomplete.');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => {
    const safeReason = /^(site_http_|oidc_|invalid_)[a-zA-Z0-9_]+$/.test(error?.message ?? '') ? error.message : 'collection_failed';
    console.error(`::error::Wallet collector ${safeReason}; no success timestamp fabricated.`);
    process.exitCode = 1;
  });
}
