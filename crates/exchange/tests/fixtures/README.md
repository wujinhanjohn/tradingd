# Captured Binance payload fixtures

Raw frames as they arrive on the combined stream endpoint
(`/stream?streams=...`), which wraps every message as
`{"stream":"<name>","data":<rawPayload>}`.

Stored verbatim, one frame per file, so the normalization tests run against the
bytes the exchange actually sends rather than against a hand-built `serde_json`
value that happens to match what we expected.

Field keys confirmed against the Binance spot WebSocket streams documentation:

- `@bookTicker` -> `u` (order book updateId), `s`, `b`, `B`, `a`, `A`.
  **No event time**: the individual book ticker stream carries no timestamp,
  which is why `normalize` takes the ingest time as a parameter.
- `@trade` -> `e`, `E` (event time), `s`, `t` (trade id), `p`, `q`,
  `T` (trade time), `m`, `M`.

Timestamps are real-looking but fixed, so the tests stay deterministic.
