# 0xStudyBank cloud scheduler

This public repository contains only the secret-free scheduler for the private
0xStudyBank dashboard. It does not contain wallet addresses, balances, API keys,
dashboard credentials, or application source code.

## Refresh cadence

- Every five minutes, one fast refresh reads the three configured exchanges and
  records exactly one portfolio snapshot.
- During the same cycle, five wallet-only micro-batches run about 50 seconds
  apart. Each batch refreshes at most three least-recently-attempted wallets.
- Two consecutive cycles therefore form a continuously repeating ten-batch
  window, enough to rotate through the current 25-wallet set without sending a
  burst to Rabby's public API.
- Each request uses a newly issued, short-lived GitHub OIDC token. No deployment
  secret is stored in this repository.

The separate monthly keepalive workflow creates a harmless heartbeat commit so
GitHub does not disable scheduled workflows after prolonged repository inactivity.
