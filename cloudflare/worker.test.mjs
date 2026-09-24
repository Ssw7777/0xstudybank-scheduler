import test from 'node:test';
import assert from 'node:assert/strict';
import { selectShard, jsonRequest, runCycle } from './worker.mjs';

test('ten shards cover all wallets exactly once, including 25 and 50 addresses', () => {
  for (const count of [1, 25, 50]) {
    const wallets = Array.from({ length: count }, (_, id) => ({ id }));
    const shards = Array.from({ length: 10 }, (_, minute) => selectShard(wallets, minute));
    assert.equal(new Set(shards.flat().map(w => w.id)).size, count);
    assert.equal(shards.flat().length, count);
    assert.ok(shards.every(shard => shard.length <= Math.ceil(count / 10)));
  }
});

test('busy persistence retries the original body and respects Retry-After', async () => {
  const waits = [], bodies = [];
  const result = await jsonRequest('https://0xstudybank.vercel.app/api/cron/wallet-observation', { method: 'POST', body: '{"observedAt":"original"}' }, 1000, {
    sleep: async ms => waits.push(ms),
    fetch: async (_, options) => {
      bodies.push(options.body);
      assert.equal(options.redirect, 'manual');
      return bodies.length === 1 ? new Response('', { status: 503, headers: { 'Retry-After': '3' } }) : Response.json({ ok: true });
    },
  });
  assert.equal(result.ok, true);
  assert.deepEqual(waits, [3000]);
  assert.equal(bodies[0], bodies[1]);
});

test('202 skipped refresh is retried, not marked complete', async () => {
  let calls = 0;
  const result = await jsonRequest('https://0xstudybank.vercel.app/api/cron/refresh', {}, 1000, {
    sleep: async () => {},
    fetch: async () => ++calls === 1 ? Response.json({ reason: 'refresh_in_progress' }, { status: 202 }) : Response.json({ ok: true }),
  });
  assert.equal(calls, 2);
  assert.equal(result.ok, true);
});

test('validation, authorization and redirect errors are never retried', async () => {
  for (const status of [401, 403, 422, 302]) {
    let calls = 0;
    await assert.rejects(jsonRequest('https://0xstudybank.vercel.app/api/cron/wallet-observation', {}, 1000, {
      fetch: async () => { calls++; return new Response('', { status }); },
      sleep: async () => assert.fail('unexpected retry'),
    }));
    assert.equal(calls, 1);
  }
});

test('retry exhaustion is bounded and does not report success', async () => {
  let calls = 0;
  await assert.rejects(jsonRequest('https://0xstudybank.vercel.app/api/cron/wallet-observation', {}, 1000, {
    sleep: async () => {}, fetch: async () => { calls++; return new Response('', { status: 503 }); },
  }));
  assert.equal(calls, 7);
});

test('public provider rate limits stop requests immediately', async () => {
  let calls = 0;
  await assert.rejects(jsonRequest('https://api.rabby.io/v1/user/total_balance', {}, 1000, {
    sleep: async () => {}, fetch: async () => { calls++; return new Response('', { status: 429 }); },
  }));
  assert.equal(calls, 1);
  await assert.rejects(jsonRequest('https://api.rabby.io/v1/user/total_balance', {}, 1000, {
    sleep: async () => assert.fail('must not retry early'), fetch: async () => new Response('', { status: 429, headers: { 'Retry-After': '120' } }),
  }), /rate_limited/);
});

test('paused wallet polling does not call the public provider but still takes scheduled snapshots', async () => {
  const original = globalThis.fetch;
  const paths = [];
  globalThis.fetch = async url => {
    paths.push(url);
    assert.equal(url, 'https://0xstudybank.vercel.app/api/cron/refresh?mode=fast');
    return Response.json({ ok: true, ran: 'fast' });
  };
  try {
    const env = { CRON_SECRET: 'test-only', WALLET_POLLING_ENABLED: 'false' };
    const idle = await runCycle(env, 60000);
    assert.equal(idle.walletPolling, 'paused_provider_rate_limit');
    assert.equal(paths.length, 0);
    const due = await runCycle(env, 300000);
    assert.equal(due.snapshot, 'accepted');
    assert.equal(due.updated, 0);
    assert.equal(paths.length, 1);
    await assert.rejects(runCycle(env, 300000, 0), /paused/);
  } finally {
    globalThis.fetch = original;
  }
});
