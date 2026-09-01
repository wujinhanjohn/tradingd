# Trading bot - agent instructions

## Project invariants (hold for every milestone, not just the current one)

- **Decimal, never float.**
  All prices, quantities, and money use `rust_decimal::Decimal`.
  `f64`/`f32` are banned in the domain and anywhere near money.
  This is enforced mechanically by `clippy.toml` (`disallowed-types`) plus the
  `clippy::float_arithmetic` lint - do not weaken either.

- **Secrets never touch logs.**
  API key and secret are wrapped in `secrecy::SecretString` and are never printed,
  `Debug`-formatted, or serialized.
  Any type holding a secret gets a hand-written `Debug` impl that redacts it.

- **Testnet by default.**
  `Env::Production` requires a loud, explicit opt-in.
  A misconfigured or ambiguous environment refuses to start.

- **Fail-closed.**
  When anything is uncertain, do nothing rather than guess.

## Crate dependency direction (never violate)

`domain` depends on nothing internal, and must stay free of `tokio`/`reqwest`/`hyper`
so both `engine` and `backtest` can share it.
`settings`, `exchange`, `strategy`, `backtest`, `engine` all depend on `domain`.
`engine` depends on `settings`, `exchange`, `strategy`.
`bot` depends on `engine`.

Libraries use typed errors (`thiserror`).
`anyhow` is for the binary and its edges only.

## Current milestone: 1 - skeleton and plumbing

Non-goals. Do NOT implement these yet:

- No Binance connection - no HTTP, no WebSocket, no `exchangeInfo`.
- No signing, no quantizer, no rate limiter.
- No order placement or cancellation logic.
- No persistence, no metrics server, no AWS.
- The `Strategy` trait is defined and a no-op stub exists, but the engine does not
  yet feed market events into it.
  Wiring that seam is a later milestone.

If you find yourself reaching for `reqwest`, `tokio-tungstenite`, or Binance types,
you have left the milestone - stop and flag it.
