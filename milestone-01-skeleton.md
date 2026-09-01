# Milestone 1 — Skeleton & plumbing

## Goal
A Cargo workspace that boots, loads config and testnet credentials, sets up structured
logging, runs an idle loop, and shuts down cleanly on Ctrl-C. **Zero network calls, zero
orders.** This is first contact with nothing — it exists to prove the plumbing.

## Non-goals (do NOT implement in this milestone)
- No Binance connection — no HTTP, no WebSocket, no `exchangeInfo`.
- No signing, no quantizer, no rate limiter.
- No order placement or cancellation logic.
- No persistence, no metrics server, no AWS.
- The `Strategy` trait is **defined** and a no-op stub exists, but the engine does **not**
  yet feed market events into it. Wiring the seam is a later milestone.

If Claude Code starts reaching for `reqwest`, `tokio-tungstenite`, or Binance types, it has
left the milestone. Stop it.

## Project invariants (hold for the entire project, not just M1)
- **Decimal, never float.** All prices, quantities, and money use `rust_decimal::Decimal`.
  `f64`/`f32` are banned in the domain and anywhere near money.
- **Secrets never touch logs.** API key/secret are wrapped in `secrecy::SecretString` and
  are never printed, `Debug`-formatted, or serialized.
- **Testnet by default.** `Production` must require a loud, explicit opt-in. A misconfigured
  or ambiguous environment refuses to start.
- **Fail-closed.** When anything is uncertain, do nothing rather than guess. (Mostly bites
  later, but bake the mindset in from crate one.)

## Workspace layout
Pick a project name and adjust as you like; this uses plain crate names. Note the config
crate is called `settings` on purpose, to avoid confusion with the `config`/`figment`
dependency it uses.

```
trading/
├── Cargo.toml            # [workspace] members
├── config.toml           # sample testnet config (checked in; NO secrets)
├── .env.example          # documents required env vars (checked in)
├── crates/
│   ├── domain/           # pure types + Strategy trait. No tokio, no I/O.
│   ├── settings/         # config + credential loading
│   ├── exchange/         # Binance adapter — EMPTY placeholder in M1
│   ├── strategy/         # Strategy implementations — NoopStrategy in M1
│   ├── backtest/         # replay harness — EMPTY placeholder in M1
│   └── engine/           # runtime/orchestrator (tokio). Owns the run loop.
└── bin/
    └── bot/              # thin binary: parse args, wire everything, call engine
```

Dependency direction (never violate): `domain` depends on nothing internal. `settings`,
`exchange`, `strategy`, `backtest`, `engine` all depend on `domain`. `engine` depends on
`settings`, `exchange`, `strategy`. `bot` depends on `engine`. `domain` must stay free of
`tokio`/`reqwest` so both `engine` and `backtest` can share it.

## Core domain types — define these now (`crates/domain`)
Bodies can be skeletal, but these signatures are the contract the rest of the project builds
against. Keep the crate pure.

```rust
use rust_decimal::Decimal;
use std::sync::Arc;

// --- primitives ---
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub Decimal);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Qty(pub Decimal);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Symbol(pub Arc<str>);            // e.g. "BTCUSDT"

/// Exchange event time, milliseconds since Unix epoch.
pub type Timestamp = i64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side { Buy, Sell }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeInForce { Gtc, Ioc, Fok }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderKind {
    Market,
    Limit { price: Price },
}

/// We generate this ourselves. It is the idempotency key: retrying an order
/// with the same id must never create a second order.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientOrderId(pub String);

// --- what a strategy asks the engine to do ---
#[derive(Clone, Debug)]
pub struct OrderIntent {
    pub client_order_id: ClientOrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub kind: OrderKind,
    pub qty: Qty,          // DESIRED size, pre-quantization. The quantizer (M3)
                           // rounds this to valid exchange step size before send.
    pub tif: TimeInForce,
}

#[derive(Clone, Debug)]
pub enum Action {
    Place(OrderIntent),
    Cancel { symbol: Symbol, client_order_id: ClientOrderId },
}

// --- what the engine feeds a strategy (grows over time) ---
#[derive(Clone, Debug)]
pub struct BookTicker {
    pub symbol: Symbol,
    pub bid: Price,
    pub bid_qty: Qty,
    pub ask: Price,
    pub ask_qty: Qty,
    pub event_time: Timestamp,
}

#[derive(Clone, Debug)]
pub struct Trade {
    pub symbol: Symbol,
    pub price: Price,
    pub qty: Qty,
    pub event_time: Timestamp,
}

#[derive(Clone, Debug)]
pub enum MarketEvent {
    BookTicker(BookTicker),
    Trade(Trade),
    // Depth, Kline, etc. added in later milestones.
}

#[derive(Clone, Debug)]
pub struct Fill {
    pub client_order_id: ClientOrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub fee: Decimal,
    pub event_time: Timestamp,
}

/// Read-only snapshot handed to the strategy on each callback. Will gain
/// positions, open orders, and a clock in later milestones.
pub struct StrategyCtx<'a> {
    pub now: Timestamp,
    _priv: std::marker::PhantomData<&'a ()>,
}

/// The seam. Everything downstream is built so a real strategy drops in here
/// later with no engine changes.
pub trait Strategy: Send {
    fn name(&self) -> &str;
    fn on_market(&mut self, event: &MarketEvent, ctx: &StrategyCtx<'_>) -> Vec<Action>;
    fn on_fill(&mut self, fill: &Fill, ctx: &StrategyCtx<'_>) -> Vec<Action> {
        let _ = (fill, ctx);
        Vec::new()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DomainError {
    #[error("invalid symbol: {0}")]
    InvalidSymbol(String),
}
```

The no-op stub lives in `crates/strategy`:

```rust
use domain::{Action, Fill, MarketEvent, Strategy, StrategyCtx};

pub struct NoopStrategy;

impl Strategy for NoopStrategy {
    fn name(&self) -> &str { "noop" }
    fn on_market(&mut self, _e: &MarketEvent, _c: &StrategyCtx<'_>) -> Vec<Action> {
        Vec::new()
    }
}
```

## Config (`crates/settings`)
Load a TOML file, allow env overrides (prefix `APP_`), via `figment`. Credentials are loaded
**separately** from env and are not part of `Config`.

```rust
#[derive(Clone, Debug, serde::Deserialize)]
pub struct Config {
    pub environment: Env,
    pub binance: BinanceConfig,
    pub logging: LoggingConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Env { Testnet, Production }

#[derive(Clone, Debug, serde::Deserialize)]
pub struct BinanceConfig {
    pub spot_rest_url: String,
    pub spot_ws_url: String,
    pub recv_window_ms: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct LoggingConfig {
    pub level: String,   // e.g. "info"
    pub json: bool,
}
```

Sample `config.toml` (testnet). **Verify these URLs against current Binance spot-testnet
docs before relying on them** — they change, and I have not confirmed them live:

```toml
environment = "testnet"

[binance]
spot_rest_url  = "https://testnet.binance.vision"
spot_ws_url    = "wss://stream.testnet.binance.vision/ws"
recv_window_ms = 5000

[logging]
level = "info"
json  = false
```

Credentials, loaded from env (`BINANCE_API_KEY`, `BINANCE_API_SECRET`):

```rust
use secrecy::SecretString;

pub struct Credentials {
    pub api_key: SecretString,
    pub api_secret: SecretString,
}
```

In M1 these come from the process environment only. AWS Secrets Manager is milestone 11.
Add a `Debug` impl (or `#[derive]` avoidance) that guarantees the secret values cannot leak;
`SecretString` already blocks accidental `Debug`/`Display`.

## Engine skeleton (`crates/engine` + `bin/bot`)
`bot/src/main.rs` wires it up:

```rust
#[derive(clap::Parser)]
struct Cli {
    #[arg(long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = <Cli as clap::Parser>::parse();
    let config = settings::load(&args.config)?;
    engine::logging::init(&config.logging)?;      // tracing-subscriber
    let creds = settings::load_credentials()?;     // from env

    // Loud guard: Production must be explicit and confirmed.
    engine::guard_environment(&config)?;

    tracing::info!(environment = ?config.environment, "starting");
    // Log a config summary here — with secrets absent by construction.

    let strategy: Box<dyn domain::Strategy> = Box::new(strategy::NoopStrategy);
    let eng = engine::Engine::new(config, creds, strategy);
    eng.run().await?;   // M1: heartbeat loop until shutdown
    Ok(())
}
```

`Engine::run` in M1 is just:

```rust
let mut hb = tokio::time::interval(std::time::Duration::from_secs(10));
loop {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutdown requested");
            break;
        }
        _ = hb.tick() => tracing::debug!("heartbeat"),
    }
}
Ok(())
```

## Dependencies (workspace)
Keep it tight. `tokio` (rt-multi-thread, macros, signal, time), `tracing` +
`tracing-subscriber` (env-filter, json), `serde` (derive), `figment` (toml + env),
`rust_decimal` (+ `rust_decimal_macros` for tests), `secrecy`, `thiserror`, `anyhow`
(binary/edges only — libraries use typed errors), `clap` (derive).

## Acceptance gate — done when all pass
- [ ] `cargo build --workspace` and `cargo clippy --workspace -- -D warnings` are clean.
- [ ] `cargo test --workspace` passes, including:
  - [ ] config parses from the sample `config.toml`;
  - [ ] a missing/invalid required field produces a clear typed error, not a panic;
  - [ ] missing `BINANCE_API_KEY`/`BINANCE_API_SECRET` fails loudly with a helpful message;
  - [ ] a test asserting a `Credentials`/config value does not appear in `Debug` output.
- [ ] Running `bot --config config.toml` (with env vars set) logs a startup line and a
      redacted config summary, emits heartbeats, and exits cleanly on Ctrl-C.
- [ ] Setting `environment = "production"` without the explicit confirmation opt-in refuses
      to start.
- [ ] `domain` has no `tokio`/`reqwest`/`hyper` in its dependency tree.

## Driving this in Claude Code
- Add a project `CLAUDE.md` that restates the four invariants above and the milestone
  non-goals, so the model self-polices scope.
- Feed it this file and instruct: implement **milestone 1 only**, stop at the gate, do not
  add networking or Binance types. Work one crate at a time, in dependency order
  (`domain` → `settings` → `strategy` stub → `engine` → `bot`), and commit at the gate.
- Suggested opening prompt: "Implement milestone 1 from `milestone-01-skeleton.md`. Build
  the workspace and the crates in dependency order. Do not touch the network or Binance.
  Stop when the acceptance gate passes and show me `cargo clippy` and `cargo test` output."
