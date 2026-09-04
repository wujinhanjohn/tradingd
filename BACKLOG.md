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

## Staleness is deliberately NOT a recorded marker - do not "fix" this

`IngestMsg::Stale` is emitted on the channel and logged, but nothing is written
to the recording, and `record::MarkerKind` deliberately stays `gap` /
`disconnect` / `reconnect`.

This looks like an omission and is not one.
A gap depends on the sequence policy that applied *at capture time*, which a
reader cannot re-derive - that is why `GapDetail` carries `SeqPolicy` into every
marker.
Staleness has no such hidden input: it is a pure function of the recording's
`recv_ns` timeline and the configured `staleness_ms`, both of which a replay
already has.
Recording it would store a derived value alongside the inputs it was derived
from, so a later change to the bound would leave the archive asserting staleness
episodes that the current configuration disagrees with.

If a future milestone genuinely needs stale markers - say, because the bound
becomes dynamic and is no longer recoverable from configuration - then the bound
in force must be recorded too, not just the verdict.

## `tokio-tungstenite` is pinned at 0.29, not 0.30 - MSRV, not inertia

0.30 requires rustc 1.85; the workspace holds `rust-version = "1.82"`, set in
milestone 1.
`cargo add` will silently pick 0.29 because of that, which is easy to mistake for
an out-of-date pin.

The ping/pong semantics are identical between the two, so nothing in the
connection layer depends on the choice.

Revisit when the toolchain for CI and the AWS image is actually chosen: if that
lands on 1.85+, raise `rust-version` and the dependency together, in one commit,
so the reason stays legible.

## The offline suite cannot fully exercise TLS - and that hid a panic

Found during the milestone-2 manual end-to-end run, not by `cargo test`.

Every socket test in this workspace runs against a local fake server over
plaintext `ws://`, which is right - it keeps the suite offline and
deterministic - but it means the first real `wss://` handshake in this project's
life happened against the live testnet.
It panicked: rustls 0.23 resolves its crypto provider from crate features and
*panics* inside the handshake if it cannot determine exactly one, and
`tokio-tungstenite`'s `rustls-tls-webpki-roots` feature does not select a
provider backend.

Fixed by depending on `rustls` directly with the `ring` feature, and by
installing that provider explicitly in `exchange::binance` so the choice is
legible in code and survives a future dependency enabling a second backend.
`crates/exchange/tests/connection.rs` now drives a real `wss://` connect against
a plain TCP listener, which walks the whole TLS setup path and asserts a typed
error rather than a panic.

**Milestone 3 added a second protocol with exactly the same hole.** The REST
tests run against a local fake HTTP server over plaintext `http://`, so the
first real `https://` handshake in this project's life also happened against the
live testnet. It worked - `ureq`'s `rustls` feature selects `ring`, and
`install_crypto_provider` is called before the first request - but that was
confirmed by a manual run, not by `cargo test`.

What is *still* not covered offline: certificate verification, the webpki root
store, and anything past the ClientHello, on either protocol.
Covering it would mean a TLS fake server with a generated CA - real work, and
worth doing before anything depends on TLS behaviour rather than merely on TLS
existing.
Until then, the manual end-to-end run against testnet is the only thing that
exercises it, and that should be stated whenever the suite is described as
covering the connection layer.

Worth remembering more generally: the fail-closed seam behaved correctly through
this. The source task panicked, the channel closed, the engine logged the dead
feed as critical and stopped cleanly, and `bot` exited non-zero carrying the real
reason. The bug was found in seconds rather than presenting as a hang.

## Filter freshness has a contract but no teeth until M6

`FilterBook::ensure_fresh(now_ns, max_age_ns)` exists, is tested, and is what
`settings.filters.max_age_ms` configures - and **nothing calls it on an order
path, because there is no order path yet**.

That is deliberate, not an oversight. The contract is defined in milestone 3 so
that milestone 6 consumes it rather than inventing one under order-path
pressure, and so the refresh loop could be built and tested against it now.

The gate item for the order milestone:

> Quantizing an order calls `ensure_fresh` first, and a stale book refuses the
> order rather than aligning it against rules we can no longer vouch for.

Note what already holds and must not regress: a *failed* refresh keeps serving
the last good book but does **not** move `fetched_at_ns`, so the book ages and
`ensure_fresh` starts failing. That is the whole fail-closed mechanism - do not
"fix" a failed refresh by restamping the book's fetch time.

## Zero in a Binance filter means "rule disabled" - do not treat it as a bound

Confirmed against the filter documentation and against the live testnet capture:
within a symbol filter, any value may be `0`, which disables that rule.
`MARKET_LOT_SIZE` on testnet BTCUSDT really does ship
`"stepSize": "0.00000000"`.

`domain::SymbolFilters` therefore accepts a zero bound and
`domain::quantize` skips the corresponding rule. This looks like a hole and is
not one: a zero `tickSize` read as a divisor is a panic, and a zero `maxQty`
read as a literal maximum rejects every order on the symbol.

What a disabled rule never does is make an order *more* valid: a price must be
strictly positive whatever `minPrice` says, and a quantity must be strictly
positive whatever `minQty` says. Both are asserted.

## The quantizer has no rounding policy, on purpose

Quantity floors; a limit price rounds to the less aggressive tick for its side
(a buy floors, a sell ceils). There is no `RoundingPolicy` parameter offering
nearest or aggressive rounding.

The reason is the project's usual one: draw the abstraction around the second
real case, not the first imagined one. No caller needs anything but the
conservative default until a real strategy does. If a milestone-6 strategy needs
aggressive quoting to get filled, add the parameter *then*, with that strategy
as the justification - and keep the conservative behaviour as the default,
because it is the safe bias for a system whose orders should be cautious.

## Deferred to M6, and already shaped for it

- **Market-order quantization.** `OrderKind::Market` returns
  `QuantizeReject::MarketNotSupported` - a loud typed refusal, not a silent
  mishandle. It needs `MARKET_LOT_SIZE` (already parsed and carried on
  `SymbolInfo::market_lot_size`, unused) and an average-price reference for the
  notional check, which needs `avgPriceMins` and a price source we do not have a
  clean answer for yet.
- **The unmodelled filters.** `PERCENT_PRICE_BY_SIDE`, `MAX_NUM_ORDERS`,
  `ICEBERG_PARTS`, `TRAILING_DELTA` and the rest are surfaced at `warn` on every
  start and otherwise not enforced. They mean the exchange can reject an order
  the quantizer thinks is fine. Whichever of them the order path trips over
  first is the one worth modelling next.

## Rate limiting: one chokepoint, no accounting yet - M4

`exchangeInfo` costs **20 request weight** against a 6000/minute budget
(confirmed from the live response's `x-mbx-used-weight` header), and the shipped
config refetches every five minutes. That is 240 weight an hour, so no limiter
is needed to make milestone 3 safe.

What matters for M4 is that there is exactly one place to put one:
`RestClient::get_json` is the single request path, and every future endpoint
goes through it. Weight accounting and the limiter slot in front of that method
rather than at call sites.

Also unimplemented and deliberately so: clock sync (`serverTime` is in the
`exchangeInfo` response we already fetch and is ignored) and signing. Neither is
needed for a public endpoint.
