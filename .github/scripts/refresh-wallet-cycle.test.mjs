import { test } from 'node:test';
import assert from 'node:assert/strict';
import { collectCycle, oldestFirst, validateManifest } from './refresh-wallet-cycle.mjs';

const wallets = Array.from({ length: 25 }, (_, i) => ({
  address: `0x${(i + 1).toString(16).padStart(40, '0')}`,
  lastSuccessAt: new Date(1000 + i * 1000).toISOString(),
  needsProtocolRefresh: true, tokenUuids: ['eth:eth'], tokenChains: ['eth'],
}));
function harness(failure) {
  const calls = [], writes = [];
  return { calls, writes, io: {
    now: () => 100_000,
    read: async (stage, wallet) => {
      calls.push({ stage, address: wallet.address });
      if (failure?.(stage, wallet, calls)) throw Object.assign(new Error('limited'), { status: 429 });
      return { body: stage === 'balance' ? { total_usd_value: 1, chain_list: [{ id: 'eth', usd_value: 1 }] } : [] };
    },
    persist: async entries => {
      assert.ok(entries.length <= 5);
      assert.equal(new Set(entries.map(e => e.address)).size, entries.length);
      writes.push(...structuredClone(entries));
    },
  } };
}
test('all 25 totals persist before any token/protocol read; late 429 cannot discard them', async () => {
  const h = harness(stage => stage === 'protocols');
  const result = await collectCycle(wallets, h.io);
  assert.equal(result.balances, 25);
  assert.equal(result.rateLimited, true);
  assert.deepEqual(h.calls.slice(0,25).map(c => c.stage), Array(25).fill('balance'));
  assert.equal(h.calls.length, 51);
  assert.equal(h.writes.filter(e => !e.tokenSpecificObservations).length, 25);
});
test('partial total batch is durable on 429 and previously skipped wallets lead next cycle', async () => {
  const h = harness((stage, w, calls) => calls.length === 4);
  const result = await collectCycle(wallets, h.io);
  assert.equal(result.balances, 3);
  assert.equal(h.writes.length, 3);
  assert.equal(h.calls.length, 4);
  const next = wallets.map(w => ({ ...w, lastSuccessAt: h.writes.find(e => e.address === w.address)?.observedAt ?? w.lastSuccessAt }));
  assert.equal(oldestFirst(next, 'lastSuccessAt')[0].address, wallets[3].address);
});
test('token failure records error, retains total observation time, stops discovery', async () => {
  const h = harness(stage => stage === 'tokens');
  const result = await collectCycle(wallets, h.io);
  assert.equal(result.balances, 25);
  assert.equal(h.calls.length, 26);
  const detail = h.writes.at(-1);
  assert.equal(detail.tokenSpecificObservations[0].tokenError, 'http_429');
  assert.equal(detail.tokenSpecificObservations[0].observation, undefined);
  assert.equal(detail.observedAt, h.writes.find(w => w.address === detail.address).observedAt);
});
test('one invalid total does not erase it or prevent other wallets', async () => {
  const h = harness();
  const read = h.io.read;
  h.io.read = (stage, w) => stage === 'balance' && w.address === wallets[0].address
    ? { body: { total_usd_value: null, chain_list: [] } } : read(stage,w);
  const result = await collectCycle(wallets, h.io);
  assert.equal(result.balances,24);
  assert.ok(h.writes.every(e => e.address !== wallets[0].address));
});
test('budget exhaustion persists collected totals without another upstream request', async () => {
  const h = harness();
  let clock = 0;
  h.io.now = () => clock;
  const read = h.io.read;
  h.io.read = async (...args) => { clock += 100; return read(...args); };
  const result = await collectCycle(wallets, h.io, { budgetMs: 250 });
  assert.equal(h.calls.length,3);
  assert.equal(h.writes.length,3);
  assert.equal(result.budgetReached,true);
});
test('manifest rejects duplicate/malformed addresses; tie ordering rotates without mutating', () => {
  assert.equal(validateManifest({ ok:true, wallets }).length,25);
  assert.throws(() => validateManifest({ ok:true, wallets: [wallets[0],wallets[0]] }));
  assert.throws(() => validateManifest({ ok:true, wallets: [{ address:'invalid' }] }));
  const tied = wallets.map(w => ({...w, lastSuccessAt:null}));
  assert.equal(oldestFirst(tied,'lastSuccessAt',3)[0].address, wallets[3].address);
  assert.equal(tied[0].address,wallets[0].address);
});
test('multiple token batches for one wallet never duplicate addresses in ingestion batch', async () => {
  const h = harness();
  const many = [{ ...wallets[0], tokenUuids: Array.from({length:201}, (_,i) => `eth:0x${i.toString(16).padStart(40,'0')}`) }];
  const result = await collectCycle(many, h.io);
  assert.equal(result.balances,1);
  assert.equal(h.writes.filter(e => e.tokenSpecificObservations).length,3);
});
test('all totals are durably written before entering the detail phase', async () => {
  const h = harness();
  const read = h.io.read;
  h.io.read = async (stage, wallet) => {
    if (stage !== 'balance') assert.equal(h.writes.filter(e => !e.tokenSpecificObservations && !e.protocolObservation && !e.tokenObservations).length,25);
    return read(stage, wallet);
  };
  await collectCycle(wallets,h.io);
});
test('failed durable write stops collection instead of claiming the cycle completed', async () => {
  const h = harness();
  h.io.persist = async () => { throw new Error('database_unavailable'); };
  await assert.rejects(collectCycle(wallets,h.io), /database_unavailable/);
  assert.equal(h.calls.length,5);
});
