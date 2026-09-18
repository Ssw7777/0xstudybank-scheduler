# 0xStudyBank cloud scheduler

This public repository contains only the secret-free scheduler for the private
0xStudyBank dashboard. It does not contain wallet addresses, balances, API keys,
dashboard credentials, or application source code.

## Refresh cadence

- Each rolling cycle refreshes the three configured exchanges and records a
  portfolio snapshot at the start and midpoint, keeping that data near a
  five-minute cadence.
- Five wallet-only micro-batches run about 135 seconds apart. Each batch
  refreshes at most five wallets, picking the wallets whose last successful
  read is oldest first, so addresses that have gone quiet are always checked
  before fresh ones.
- After the rolling batches, the run reads the dashboard status endpoint and
  runs up to three extra stale-only batches (60 seconds apart) for any wallet
  that still has not succeeded in over 20 minutes, until the queue drains.
  If the public balance providers keep rate-limiting, the dashboard itself
  falls back to an authenticated provider for the worst wallets (daily quota
  capped on the dashboard side).
- A completed run uses its short-lived GitHub token to queue the next run. A
  six-hour cron acts only as a watchdog because GitHub cron delivery can be delayed.
- Each request uses a newly issued, short-lived GitHub OIDC token. No deployment
  secret is stored in this repository.

The separate monthly keepalive workflow creates a harmless heartbeat commit so
GitHub does not disable scheduled workflows after prolonged repository inactivity.
