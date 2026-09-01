# Backlog

Things deliberately deferred, with the reason, so a later milestone can pick them
up without rediscovering why they matter.

## Graceful shutdown on SIGTERM - gate item for the risk milestone

Milestone 1 handles Ctrl-C (SIGINT) only, which is what the M1 spec asked for and
is sufficient while the process has no state to flush.

This stops being cosmetic the moment there are open orders. Container
orchestrators - ECS, Kubernetes - send **SIGTERM**, not SIGINT, on both shutdown
and deploy. A process that only listens for SIGINT gets killed outright on every
deploy once it is running on AWS, skipping any clean cancel-all path.

When the risk layer is specced, this should be an acceptance-gate item:

> Graceful shutdown on SIGTERM cancels all open orders and flattens positions
> before exiting, within the orchestrator's termination grace period.

Note the grace period is finite (ECS `stopTimeout`, k8s
`terminationGracePeriodSeconds`, both commonly 30s), so the cancel-all path needs
its own deadline and a fail-closed answer for what happens if it does not finish.

## Environment and endpoint are not cross-checked - M2

`Config::validate` checks that `spot_rest_url` is `https://` and `spot_ws_url` is
`wss://`, but nothing checks that the URLs *match the declared environment*.

Surfaced while testing the production guard: a config saying
`environment = "production"` while pointing at `testnet.binance.vision` starts
happily and prints the production banner. The reverse - `environment = "testnet"`
aimed at the live endpoint - is the dangerous direction, and it would also pass.

Milestone 1 cannot fix this honestly, because knowing the real production host is
Binance-specific knowledge that belongs in the `exchange` crate. When M2 defines
those endpoints, `validate` should reject a mismatch: production must point at
production hosts, testnet at testnet hosts, and anything else refuses to start.
Until then the guard protects the *declared* environment, not the *actual*
destination, and that distinction should not be forgotten.

## Corrected crate dependency direction

The M1 spec says `engine` depends on `settings`, `exchange`, `strategy`. The
`strategy` edge is wrong and is not implemented: it would be unused, and an
unused edge to a concrete strategy crate is the affordance that lets someone
reach for `NoopStrategy` in a hurry.

Actual direction, which later milestones should preserve:

- `engine` -> `{domain, settings}`
- `bot` is the only crate that names a concrete strategy, and injects it as
  `Box<dyn domain::Strategy>`
