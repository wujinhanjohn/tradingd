# Milestone 2 — Market data ingest (read-only)

## Read this first: current state at end of milestone 1
This spec is self-contained; you do not need prior context. The workspace already exists and
M1 is committed to git. Before writing code, read the repo's `CLAUDE.md` and `BACKLOG.md`.
Facts about what is already built that this milestone depends on:

- Crates: `domain` (pure types + `Strategy` trait, no tokio/network — deps are only
  `rust_decimal` + `thiserror`), `settings` (figment TOML + env config), `strategy`
  (`NoopStrategy` stub), `engine` (tokio run loop, env guard, logging), and `bin/bot` (wires
  it together).
- **Dependency direction as built (not the original written spec):** `strategy → domain`;
  `engine → {domain, settings}`; `bot → {domain, engine, settings, strategy}`. **`engine`
  does NOT depend on `strategy`** — `bot` is the only crate that names a concrete strategy
  and injects it as `Box<dyn Strategy>`. Preserve this: the engine must never be able to
  resolve a concrete implementation.
- `domain` re-exports its public dependency types: `pub use rust_decimal::Decimal;`. Any
  crate touching `Decimal` in a domain signature uses `domain::Decimal`, not its own
  `rust_decimal` import.
- No-float rule is compiler-enforced via `clippy.toml` (`disallowed-types = ["f32","f64"]`)
  plus workspace lints (`float_arithmetic`, `unsafe_code = "forbid"`). Binance sends numeric
  fields as JSON strings; parse them straight to `Decimal`, never through `f64`.
- Environment type `Env { Testnet, Production }` lives in `settings`. Production requires the
  env var `ALLOW_PRODUCTION=I_UNDERSTAND_THE_RISK` (this is deliberately NOT `APP_`-prefixed,
  because `APP_` is figment's config-override prefix and the arming switch must live outside
  the config namespace). Config parsing uses `deny_unknown_fields` and `Toml::file_exact`.

**This milestone modifies committed crates** (`settings`, `engine`, `bot`) and **adds one
crate** (`exchange`). Because M1 is committed, touching it is safe and recoverable.

## Goal
Connect to the Binance testnet spot WebSocket, normalize messages into the `domain` market
types, stamp them with an ingest time, detect gaps and staleness, reconnect on drop, and
record the raw stream to disk in a replay-shaped format. The running `bot` now carries a live
market feed and logs it. **Read-only** throughout: no auth, no signing, no orders, no REST.
The engine still does **not** route events to the strategy (that is M5).

## Non-goals (do NOT implement here)
- No signing/auth — public market streams are unauthenticated.
- **No REST client.** M2 is WebSocket-only; `reqwest`/`hyper`/`ureq` must still be absent
  from the workspace. (`exchangeInfo` and filters are M3 and bring the first REST call.)
- No `exchangeInfo`, symbol filters, or quantizer (M3). No order placement (M6). No strategy
  routing (M5). No Postgres (M9) — disk recording here is local files, a different concern.
  No metrics server (M10) — health is surfaced via `tracing` for now.

## Invariants
Decimal-not-float, secrets-never-logged, testnet-by-default, and fail-closed all still hold.
One addition, load-bearing for the whole research loop:

- **Normalization is a pure function.** The exact same code that turns a raw Binance payload
  into a `domain::MarketEvent` runs on live ingest and on replay. It may not touch the
  network, the wall clock, or mutable global state. This is what makes a recorded session
  replay identically to how it ran live.

## New crate: `exchange`
The Binance adapter. Dependency direction: `exchange → domain` (+ external crates). It must
**not** depend on `settings` (see the endpoint check below for how it stays decoupled).

- External deps: `tokio` (features `time`, `sync` only — the runtime flavour is `bot`'s, not
  this crate's), `tokio-tungstenite` (WebSocket; pulls tokio `net` + a TLS backend
  transitively — prefer `rustls`), `futures-util`, `serde` + `serde_json` (parse payloads,
  write NDJSON records), `thiserror`, `tracing`.
- **Note on the dep-tree audit:** `wss://` requires TLS, so `rustls` legitimately enters the
  tree here. The "no HTTP client" property is specifically about `reqwest`/`hyper`/`ureq`,
  which stay absent. State it that way in the gate check.
- Suggested modules: `endpoint` (canonical hosts + classification), `normalize` (pure
  raw→`MarketEvent`), `record` (NDJSON writer + reader), `binance` (connection, subscription,
  ping/pong, reconnect/backoff), `source` (public entry point that ties it together and
  produces the receiver the engine reads).

## Endpoint / environment cross-check (the M1 backlog item)
The dangerous config error in this whole system is a testnet-labelled config pointing at a
live host — it passes the M1 env guard and then trades real funds. M1 could not fix it
because canonical hostnames are exchange knowledge. Fix it here, structurally:

- `exchange` defines `EndpointClass { Testnet, Production }`, the canonical host constants,
  and `classify(url) -> Option<EndpointClass>` (inspects the host).
- The source constructor takes the expected class and validates it:
  `BinanceMarketSource::connect(expected: EndpointClass, ws_url: &str, ...)` returns an
  `EndpointMismatch` error and does **not** connect unless `classify(ws_url) == Some(expected)`.
  You cannot construct a source aimed at the wrong environment.
- `bot` maps `settings::Env` → `EndpointClass` and passes it in.
- **Design note:** I considered moving `Env` into `domain` so the check could be typed by the
  real environment enum, and chose not to — it would churn three committed crates (`domain`,
  `settings`, `engine`) for a mapping that is one trivial `match` in `bot`, and the
  constructor still validates structurally either way. If you'd rather centralise `Env` in
  `domain`, that's a defensible alternative; it's a pure relocation with a re-export, but it's
  not required for this milestone.
- Test **both** mismatch directions. The one that must never regress: `env = testnet` +
  production host → refuse.

## Recording format — get this right now, the backtester depends on it
Record **raw inbound payloads plus ingest metadata, not normalized events.** Rationale:
normalization is a pure function applied identically on live and replay, so recording raw
means a later normalization fix re-derives correct events from history instead of leaving you
with a corrupted archive. Replay becomes: read raw records → run the same `normalize` →
feed the engine, on a byte-identical path to live.

- Format: newline-delimited JSON (JSON Lines), append-only, one file per session. Filename
  includes the UTC start time and the symbol set. Uncompressed in M2 for debuggability;
  compression is a later concern.
- Data line schema (one JSON object per line):
  ```json
  {"recv_ns": 1712345678901234567, "seq": 42, "stream": "btcusdt@bookTicker", "payload": { /* raw Binance JSON, verbatim */ }}
  ```
  `recv_ns` is our ingest clock (i64 nanoseconds since the Unix epoch); `seq` is a monotonic
  counter we assign on ingest (distinct from any exchange-side id); `payload` is the raw
  message unchanged.
- Health markers are recorded **inline** so replay reproduces the same gap flags:
  ```json
  {"recv_ns": ..., "seq": ..., "marker": "gap", "detail": {"stream": "...", "from": 100, "to": 137}}
  ```
  with `"marker"` also taking `"disconnect"` / `"reconnect"`.
- **Fail-closed:** if recording is enabled and the target directory is not writable (or the
  file can't be opened), the source refuses to start rather than silently running unrecorded.

## Gap, staleness, and connection health
- **Gap:** track the per-stream update/sequence id carried in the payload (e.g. bookTicker's
  update id, trade id — confirm exact field keys, see Verify list) and flag non-contiguous
  jumps. Emit `IngestMsg::Gap` and write a gap marker.
- **Staleness:** if a subscribed stream sees no message within `staleness_ms` (config), emit
  `IngestMsg::Stale`. A liquid pair like BTCUSDT ticks sub-second, so a few seconds of silence
  is already anomalous — pick a conservative default and make it configurable.
- **Keepalive:** Binance sends WS ping frames that must be answered with pong or the server
  drops you (confirm the current interval/behaviour and that the tungstenite layer's auto-pong
  is actually engaged, else handle it manually). Binance also force-closes connections
  periodically — reconnect is mandatory, not a nicety.
- **Reconnect:** on any disconnect, reconnect with capped exponential backoff **plus jitter**
  (never hot-loop). On reconnect, emit `Connected` and flag the gap spanning the outage.

## `domain` additions (minimal, still pure)
- Add an `IngestMsg` enum — the shape of what flows from any market source to the engine:
  `Market(MarketEvent)`, `Gap { stream, detail }`, `Stale { stream, since_ns }`,
  `Connected`, `Disconnected { reason }`. Plain data: **no serde, no async**, so `domain`
  stays on `rust_decimal` + `thiserror` with nothing new. (The recording layer serialises raw
  payloads in `exchange`, not these types, so `domain` needs no `serde`.)
- No `Env` move. No new `domain` dependencies.

This `IngestMsg` channel — not a trait — is the seam between a market source and the engine,
mirroring how `bot` injects the strategy. Don't add a `MarketSource` trait yet; introduce it
only when the second implementation (the replay source) arrives, so the abstraction is drawn
around two real cases rather than one imagined one.

## `settings` additions
- `[market]` section: `symbols` (validated through `domain::Symbol::new`), the stream kinds to
  subscribe (book ticker, trades), and `staleness_ms`.
- `[recording]` section: `enabled` (bool) and `dir` (path).
- Keep `deny_unknown_fields`; add the new fields to the shipped `config.toml`; range/shape-
  validate as elsewhere (bounded `staleness_ms`, non-empty `symbols`).

## `engine` changes
- `Engine::new` gains a `market_rx: tokio::sync::mpsc::Receiver<domain::IngestMsg>` parameter.
  `engine` keeps its deps (`domain`, `settings`, tokio) and does **not** gain a dependency on
  `exchange`.
- The `select!` run loop gets an arm reading `market_rx`: log `Market` events at `debug`/
  `trace` (still **not** routed to the strategy), `Stale`/`Disconnected` at `error`,
  `Connected` at `info`. Keep a light last-event-time for logging.
- **Fail-closed:** if `market_rx` closes (the source task died), treat it as critical — log
  at `error` and shut down cleanly. A running engine with a dead feed is exactly the silent
  degradation this project refuses; with nothing to trade in M2, "halt" means a clean stop.

## `bot` changes
- Build the exchange config from `settings` (symbols, streams, `staleness_ms`, ws url,
  recording config).
- Map `settings::Env → EndpointClass`, call `BinanceMarketSource::connect(...)` (which
  validates the endpoint), spawn the source task on the runtime, and pass its `Receiver` to
  `Engine::new`. `bot` remains the sole place that wires concrete implementations — now the
  market source as well as the strategy.

## Testing — offline and deterministic
The live testnet is for the manual end-to-end demo **only**, never inside `cargo test`. Live
feeds are non-deterministic, rate-limited, and flaky; a suite that leans on them rots.

- Stand up a local fake WebSocket server in tests (`tokio-tungstenite` bound to
  `127.0.0.1:0`) and script it: emit known frames (assert normalization), emit an
  out-of-order / missing sequence (assert `Gap`), go silent (assert `Stale`, using
  `tokio::time` paused virtual time so there's no real waiting), drop the connection (assert
  reconnect-with-backoff and the gap across the outage), send a ping (assert the pong).
- Normalization unit tests: feed a handful of captured raw Binance JSON samples stored as
  fixtures; assert the exact `domain::MarketEvent`, including `Decimal` parsed from the string
  fields (never via `f64`).
- Recording round-trip: record a scripted session, read the file back, normalize, and assert
  you recover the same event sequence **and** the same gap markers — this is the proof that
  the file is replay-shaped.
- Endpoint check: both mismatch directions refuse; the matching direction connects (to the
  fake server), with `classify` covered by unit tests over the canonical constants.

## Acceptance gate — done when all pass
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean; no-float rule still holds
      (Binance decimal strings parsed straight to `Decimal`).
- [ ] Workspace has **no HTTP client** (`reqwest`/`hyper`/`ureq`); `rustls` via
      `tokio-tungstenite` is present and expected. Confirm by dependency-tree grep.
- [ ] Normalization is a pure function with fixture tests; decimals exact.
- [ ] Gap, staleness, and reconnect-with-backoff are all exercised against a local fake WS
      server, deterministically (paused time for staleness), with zero live network in
      `cargo test`.
- [ ] Recording writes replay-shaped NDJSON including health markers; the round-trip test
      reproduces the same event + gap sequence. Recording-enabled-with-unwritable-dir refuses
      to start.
- [ ] Endpoint/env cross-check refuses **both** mismatch directions (especially
      testnet-label → prod-host) and is structural in the source constructor.
- [ ] `engine` shuts down cleanly (does not hang) when the market source dies.
- [ ] `domain` still depends only on `rust_decimal` + `thiserror`.
- [ ] Manual E2E: `bot` with a testnet config connects, logs a live normalized
      BookTicker/Trade stream with ingest timestamps, writes a recording file, and stops
      cleanly on Ctrl-C. Capture the log and a few recorded lines.

## Verify against current Binance docs before relying on any of it
These change and I have not confirmed them live — treat as items to check, not as given:
- Testnet spot WS base URL (config currently ships `wss://stream.testnet.binance.vision/ws`);
  confirm host, `/ws` vs `/stream`, and the combined-stream `{stream, data}` wrapping.
- Exact stream names and payload field keys for bookTicker and trade/aggTrade, including which
  field is the update/sequence id usable for gap detection.
- WS keepalive: current server ping interval, the client pong requirement, and the maximum
  connection lifetime before a forced disconnect.
- **`data-stream.binance.vision` is classified as PRODUCTION in `exchange::endpoint`.**
  Confirm this against current Binance docs before trusting it.
  It shares the `binance.vision` suffix with every testnet host but the spot docs describe it
  as a production market-data-only endpoint, so a suffix-based classifier would read the live
  feed as testnet.
  Tests cannot settle this - they only assert the classification we wrote down.
  If Binance ever repurposes this host, the constant in `PRODUCTION_HOSTS` is what must change.
- Confirm public market streams need no `listenKey` on testnet (they shouldn't — `listenKey`
  and the User Data Stream are an M7 auth concern).

## Driving this in a fresh Claude Code session
Build the `exchange` crate inside-out: `endpoint`, `normalize`, and `record` are all pure and
offline — implement and fully unit-test them **before** any socket code, so the deterministic
core is proven before a network connection exists. Then the connection/reconnect layer against
the fake server. Then the `settings` / `engine` / `bot` wiring. Stop for review at the end of
the `exchange` crate, again after the wiring, then run the gate.

Suggested opening prompt:

```
Read milestone-02-market-data.md, plus CLAUDE.md and BACKLOG.md, and treat the spec as
authoritative. Implement milestone 2 only: read-only Binance testnet market-data ingest.

Scope guards: WebSocket only — no REST client (reqwest/hyper/ureq must stay absent), no
auth/signing, no orders, no strategy routing. Normalization must be a pure function usable
by both live ingest and later replay.

Invariants: rust_decimal for all numbers (parse Binance's JSON string fields straight to
Decimal, never via f64); secrets never logged; testnet-default; fail-closed. Keep the
as-built dependency direction — engine must not depend on the exchange or strategy crates;
bot injects both concrete implementations.

Build the new `exchange` crate inside-out and stop for my review after each stage:
  1. endpoint (canonical hosts + EndpointClass + classify + structural mismatch refusal)
  2. normalize (pure raw-JSON -> domain::MarketEvent, fixture-tested, decimals exact)
  3. record (append-only NDJSON of raw payloads + ingest metadata + inline health markers;
     record/read round-trip test; refuse to start if an enabled recording dir is unwritable)
  4. connection layer (subscribe, ping/pong keepalive, gap + staleness detection, reconnect
     with backoff+jitter) tested against a LOCAL fake WS server — never the live testnet
Then wire it up: domain::IngestMsg enum, settings [market]/[recording] sections, the engine
run-loop arm (log events, fail-closed clean-shutdown if the feed dies), and bot spawning the
validated source and passing the receiver to the engine.

All tests must be offline and deterministic (fake WS server, tokio paused time for
staleness). The live testnet is for the manual end-to-end demo only, not for cargo test.

Definition of done is the acceptance gate in the spec. Do not mark M2 complete until every
item passes, and show me cargo clippy and cargo test output plus the dep-tree grep proving no
HTTP client. Do not start milestone 3.
```
