# 0xStudyBank cloud scheduler

This public repository contains only the secret-free scheduler for the private
0xStudyBank dashboard. It does not contain wallet addresses, balances, API keys,
dashboard credentials, or application source code.

## Current state (2026-09-25)

Cloudflare now checks the existing GitHub wallet workflow every minute and wakes
it only when data is due and no run is queued/running. The GitHub workflow keeps
collecting data; Cloudflare does not retry its rate-limited direct Rabby path.
Wallet-only dispatch skips the competing exchange refresh. Failed workflows have
a ten-minute dispatch cooldown. Primary shards run sequentially; HTTP 429 stops
the run instead of retrying from another runner. Tokens are Worker secret bindings.

Protocol-detail requests are due after eight minutes, independently of the
5.5-hour authenticated App-audit quota. This is an intended refresh cadence,
not an upstream availability guarantee. Real-time acceptance still requires
observing complete autonomous rounds.

## Previous diagnosis (2026-09-24)

- Cloudflare Worker `studybank-refresh` runs a one-minute cron independently of
  the owner's computer. Every fifth minute it requests a Vercel fast refresh:
  exchange balances and a portfolio snapshot. This path has been observed running.
- **Wallet migration is not complete.** The public Rabby endpoint returned HTTP
  429 on consecutive autonomous invocations after slowing sequential requests.
  Successful manual invocations did not establish that scheduled invocations work.
- `WALLET_POLLING_ENABLED=false` stops failing Cloudflare wallet requests. It does
  not stop fast refreshes or claim that cached wallet values are fresh.
- Existing GitHub workflows remain as a best-effort fallback. Delayed schedules
  do not provide a strict ten-minute freshness guarantee.
- Do not remove that fallback or claim full migration until a permitted,
  adequately provisioned data source passes autonomous-cycle acceptance tests.

## Prepared wallet implementation

When enabled, ten static shards cover all configured addresses every ten minutes
(up to 50 addresses). Balance and protocol requests are sequential with four-second
spacing. HTTP 429 stops that shard without immediately retrying the rate limit.
Other transient source failures receive one retry. Failed reads never become zero.

Vercel writes have bounded backoff and reuse the original observations and timestamps.
A skipped HTTP 202 refresh is not completion. Partial cycles log failure counts and
fail the scheduled invocation rather than reporting false success.

`CRON_SECRET` must be a Worker secret binding, never a source-code value.
`wrangler.jsonc` contains only a non-secret feature flag.

## Tests and acceptance

Run `node --test cloudflare/worker.test.mjs`.

Before enabling wallet polling, observe at least two autonomous full cycles and
check every wallet's actual success timestamp and protocol timestamp. Inspect the
application logs, not merely invocation counts. Compare supported wallets with an
independent source. Do not interpret balance differences as transfers.

## Capacity

At 25 wallets, ten-minute polling requires 3,600 balance requests/day and another
3,600 protocol requests/day, excluding retries. Fast refreshes are 288/day; cron
invocations are 1,440/day. A free scheduling quota is not an upstream data quota.
Real transaction history needs separate adequate query capacity or production webhooks.
