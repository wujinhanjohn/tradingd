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

## `exchange_info/` - `exchangeInfo` fixtures (milestone 3)

A different payload shape for a different parse, so they live in their own
directory: the stream-frame sweep in `normalize_fixtures.rs` reads only the top
level.

`exchange_info/btcusdt.json` is a **verbatim capture** of

```
GET https://testnet.binance.vision/api/v3/exchangeInfo?symbol=BTCUSDT
```

taken on 2026-09-04, re-serialised onto one line and otherwise untouched. It is
the ground truth for what the response actually looks like, including the parts
this build does not model.

Confirmed from it, and against the current filter documentation:

- Every numeric filter field is a JSON **string**. Integer-valued fields on
  filters we do not model (`avgPriceMins`, `maxNumOrders`) are bare numbers,
  which is why the string requirement is enforced per field rather than per
  payload.
- Spot BTCUSDT carries `NOTIONAL` (`minNotional`, `applyMinToMarket`,
  `maxNotional`, `applyMaxToMarket`, `avgPriceMins`), **not** the legacy
  `MIN_NOTIONAL`.
- `MARKET_LOT_SIZE` really does ship `"stepSize": "0.00000000"`, which is
  Binance's spelling for "this rule is disabled" - the reason a zero bound is
  modelled rather than refused.
- Unmodelled filter types present on this one symbol: `ICEBERG_PARTS`,
  `TRAILING_DELTA`, `PERCENT_PRICE_BY_SIDE`, `MAX_NUM_ORDERS`,
  `MAX_NUM_ORDER_LISTS`, `MAX_NUM_ALGO_ORDERS`, `MAX_NUM_ORDER_AMENDS`.

The rest are derived from that capture, each changing exactly one thing:

- `two_symbols.json` - a second symbol carrying the legacy `MIN_NOTIONAL` filter
  and a non-`TRADING` status.
- `bare_number.json` - `tickSize` as a bare JSON number, the banned float path.
- `missing_lot_size.json` - a symbol with no `LOT_SIZE` filter.
- `future_fields.json` - a filter type we have never heard of, plus an additive
  new field inside one we do model.
