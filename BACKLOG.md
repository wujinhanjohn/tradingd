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

## Gap detection is per-stream by sequence SEMANTICS - correction to the M2 spec

The M2 spec says to "flag non-contiguous jumps" in the per-stream update/sequence
id, as though one rule covered every stream.
It does not, and implementing it that way would have made gap alerting useless.

Confirmed against the Binance spot WebSocket docs during milestone 2:

- `@trade` carries `t`, the per-symbol trade id.
  It increments by exactly **one** per trade, so a jump is a real gap and the
  number of missed messages is countable.
  This is `SeqPolicy::Contiguous`.
- `@bookTicker` carries `u`, the **order book updateId**.
  It is monotonic but *not* contiguous: it counts book updates, not pushed
  messages, so it jumps by arbitrary amounts as a matter of course.
  This is `SeqPolicy::Monotonic`, and only a repeat, a decrease, or an
  outage-spanning jump is evidence of anything.

Treating `u` as contiguous would fire a gap alert on nearly every message, which
is worse than no alerting at all - it trains an operator to ignore the one signal
that says data is missing.

`exchange::SeqPolicy` carries this per stream kind, and it is recorded in every
gap marker so a replay reconstructs gaps under the semantics that actually
applied rather than re-deriving them under a single wrong rule.

**This must hold through the connection layer (stage 4) and into replay.**
A monotonic-stream marker and a contiguous-stream marker are not interchangeable.
Any later stream kind added to `StreamKind` has to declare its policy, and the
`e.g. bookTicker's update id, trade id` phrasing in the M2 spec should not be read
as saying the two behave alike.
