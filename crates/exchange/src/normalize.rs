//! Raw Binance payload -> [`domain::MarketEvent`]. **Pure**, by construction.
//!
//! This module is the load-bearing piece of the whole research loop. The exact
//! same code runs on live ingest and on replay, so a recorded session replays
//! identically to how it ran. That only holds if normalization is a function of
//! its arguments and nothing else, so this module:
//!
//! - reads no clock - the ingest time arrives as the `recv_ns` parameter,
//! - touches no network,
//! - holds no mutable global state,
//! - and does not allocate an identity, a random value, or anything else that
//!   could differ between two runs over the same input.
//!
//! # Decimals, and how the f64 ban is enforced here
//!
//! Binance sends every numeric market field as a JSON **string**. We require
//! that: a numeric field arriving as a JSON *number* is rejected rather than
//! read, because `serde_json` stores a non-integral JSON number as an `f64` and
//! reading it back is exactly the float path this project bans. Strings go
//! through [`Decimal::from_str_exact`], which refuses to round rather than
//! silently truncating past 28 significant digits.

use domain::{BookTicker, Decimal, MarketEvent, Price, Qty, Symbol, Timestamp, Trade};
use serde_json::Value;

/// The market streams this milestone understands.
///
/// Deliberately closed. An unrecognised stream is an error, not something we
/// pass through half-parsed - if we subscribed to something we cannot read, the
/// bug is in the subscription, and it should be loud.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreamKind {
    /// `<symbol>@bookTicker` - best bid/ask, pushed on every book change.
    BookTicker,
    /// `<symbol>@trade` - individual trades.
    Trade,
}

/// What contiguity the ordering id of a stream actually offers.
///
/// This distinction is not pedantry - it decides what a "gap" even means:
///
/// - `@trade` carries `t`, the per-symbol trade id, which increments by exactly
///   one. A jump of `n` means we missed `n - 1` trades and we can say so.
/// - `@bookTicker` carries `u`, the *order book* updateId. It increases, but by
///   arbitrary amounts, because it counts book updates rather than pushed
///   messages. A jump there is normal and means nothing. Only a repeat or a
///   decrease is evidence of a problem.
///
/// Treating `u` as contiguous would produce a gap alert on essentially every
/// message, which is worse than no alerting at all: it trains you to ignore it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SeqPolicy {
    /// Increments by exactly one per message. Missing values are detectable.
    Contiguous,
    /// Increases by arbitrary amounts. Only regressions are detectable.
    Monotonic,
}

/// The ordering id carried by a payload, plus what we may infer from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Seq {
    pub id: i64,
    pub policy: SeqPolicy,
}

/// A stream name, parsed. Binance spells these `btcusdt@bookTicker`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamId {
    pub symbol: Symbol,
    pub kind: StreamKind,
}

/// The result of normalizing one payload: the domain event, and the ordering id
/// the connection layer uses for gap detection.
///
/// The two travel together on purpose. Gap detection has to run on replay
/// exactly as it ran live, so the id must come out of the same pure parse as the
/// event rather than being re-derived by whoever happens to be reading.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Normalized {
    pub event: MarketEvent,
    pub seq: Seq,
}

/// Every way a payload can fail to become an event. All of them mean "drop this
/// message and say so", never "guess".
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum NormalizeError {
    #[error("malformed stream name `{stream}`: expected `<symbol>@<kind>`")]
    MalformedStreamName { stream: String },

    #[error("stream `{stream}` is of unsupported kind `{kind}`; this build handles {supported}")]
    UnsupportedStreamKind {
        stream: String,
        kind: String,
        supported: &'static str,
    },

    #[error("stream `{stream}` names invalid symbol `{symbol}`")]
    InvalidSymbol { stream: String, symbol: String },

    #[error("`{stream}`: payload is {found}, expected an object")]
    NotAnObject { stream: String, found: &'static str },

    #[error("`{stream}`: missing field `{field}`")]
    MissingField { stream: String, field: &'static str },

    #[error("`{stream}`: field `{field}` is {found}, expected {expected}")]
    WrongType {
        stream: String,
        field: &'static str,
        expected: &'static str,
        found: &'static str,
    },

    #[error(
        "`{stream}`: field `{field}` = `{value}` is not an exact decimal. \
         Values are parsed with no rounding; a value we cannot represent \
         exactly is dropped rather than approximated"
    )]
    NotADecimal {
        stream: String,
        field: &'static str,
        value: String,
    },

    #[error("`{stream}`: field `{field}` = `{value}` is out of range ({reason})")]
    OutOfRange {
        stream: String,
        field: &'static str,
        value: String,
        reason: &'static str,
    },

    #[error(
        "`{stream}`: payload is for symbol `{payload_symbol}` but the stream is \
         for `{stream_symbol}`. Refusing to route one symbol's prices under \
         another's name"
    )]
    SymbolMismatch {
        stream: String,
        stream_symbol: String,
        payload_symbol: String,
    },

    #[error("`{stream}`: payload event type is `{found}`, expected `{expected}`")]
    EventTypeMismatch {
        stream: String,
        expected: &'static str,
        found: String,
    },

    #[error("ingest timestamp {recv_ns}ns is negative; refusing to stamp an event with it")]
    InvalidIngestTime { recv_ns: i64 },
}

impl StreamKind {
    /// Every kind this build supports, in a stable order.
    pub const ALL: &'static [Self] = &[Self::BookTicker, Self::Trade];

    /// The suffix Binance uses in a stream name. Case matters: Binance spells it
    /// `bookTicker`, and that exact spelling goes into SUBSCRIBE requests.
    #[must_use]
    pub fn suffix(self) -> &'static str {
        match self {
            Self::BookTicker => "bookTicker",
            Self::Trade => "trade",
        }
    }

    /// The `"e"` (event type) value Binance stamps on this stream's payloads, if
    /// it stamps one at all. `@bookTicker` payloads carry no event type.
    #[must_use]
    pub fn event_type(self) -> Option<&'static str> {
        match self {
            Self::BookTicker => None,
            Self::Trade => Some("trade"),
        }
    }

    /// What contiguity this stream's ordering id offers. See [`SeqPolicy`].
    #[must_use]
    pub fn seq_policy(self) -> SeqPolicy {
        match self {
            Self::BookTicker => SeqPolicy::Monotonic,
            Self::Trade => SeqPolicy::Contiguous,
        }
    }

    /// The stream name to subscribe to, e.g. `btcusdt@bookTicker`. Binance
    /// requires the symbol lowercased here even though it echoes it uppercased
    /// in the payload.
    #[must_use]
    pub fn stream_name(self, symbol: &Symbol) -> String {
        format!("{}@{}", symbol.as_str().to_ascii_lowercase(), self.suffix())
    }

    fn from_suffix(suffix: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|k| k.suffix() == suffix)
    }

    fn supported_list() -> &'static str {
        "`bookTicker` and `trade`"
    }
}

/// Parse a Binance stream name into a symbol and a kind.
///
/// # Errors
///
/// Fails on a name without exactly one `@`, on a symbol `domain` will not vouch
/// for, and on a stream kind this build does not implement.
pub fn parse_stream(stream: &str) -> Result<StreamId, NormalizeError> {
    let (raw_symbol, suffix) = stream
        .split_once('@')
        // Split on the *first* `@` only. Binance's own names can carry a second
        // one (`btcusdt@depth20@100ms`), so everything after the first is the
        // kind - which makes an unsupported stream report as unsupported rather
        // than as malformed.
        .filter(|(sym, suffix)| !sym.is_empty() && !suffix.is_empty())
        .ok_or_else(|| NormalizeError::MalformedStreamName {
            stream: stream.to_owned(),
        })?;

    // Binance lowercases the symbol in stream names and uppercases it in
    // payloads. `domain::Symbol` only accepts uppercase, which is the canonical
    // form everywhere above this line.
    let symbol = Symbol::new(&raw_symbol.to_ascii_uppercase()).map_err(|_| {
        NormalizeError::InvalidSymbol {
            stream: stream.to_owned(),
            symbol: raw_symbol.to_owned(),
        }
    })?;

    let kind =
        StreamKind::from_suffix(suffix).ok_or_else(|| NormalizeError::UnsupportedStreamKind {
            stream: stream.to_owned(),
            kind: suffix.to_owned(),
            supported: StreamKind::supported_list(),
        })?;

    Ok(StreamId { symbol, kind })
}

/// Turn one raw Binance payload into a [`domain::MarketEvent`].
///
/// `stream` is the Binance stream name (`btcusdt@trade`); `payload` is the raw
/// message body, verbatim, exactly as it is stored in a recording; `recv_ns` is
/// our ingest clock in nanoseconds since the Unix epoch, passed in rather than
/// read, which is what keeps this function pure and replayable.
///
/// The payload's own `"s"` field is cross-checked against the stream name. A
/// disagreement is refused rather than resolved: routing one symbol's prices
/// under another symbol's name is the kind of error that shows up later as an
/// inexplicable position.
///
/// # Errors
///
/// See [`NormalizeError`]. Every variant is a dropped message with a reason,
/// never a partially-populated event.
pub fn normalize(
    stream: &str,
    payload: &Value,
    recv_ns: i64,
) -> Result<Normalized, NormalizeError> {
    let id = parse_stream(stream)?;

    if !payload.is_object() {
        return Err(NormalizeError::NotAnObject {
            stream: stream.to_owned(),
            found: type_name(payload),
        });
    }

    // The payload echoes its own symbol; it must be the one we asked for.
    let payload_symbol = str_field(stream, payload, "s")?;
    if !payload_symbol.eq_ignore_ascii_case(id.symbol.as_str()) {
        return Err(NormalizeError::SymbolMismatch {
            stream: stream.to_owned(),
            stream_symbol: id.symbol.as_str().to_owned(),
            payload_symbol: payload_symbol.to_owned(),
        });
    }

    // Where a payload declares its own event type, it must agree with the stream
    // name we filed it under.
    if let Some(expected) = id.kind.event_type() {
        let found = str_field(stream, payload, "e")?;
        if found != expected {
            return Err(NormalizeError::EventTypeMismatch {
                stream: stream.to_owned(),
                expected,
                found: found.to_owned(),
            });
        }
    }

    match id.kind {
        StreamKind::BookTicker => normalize_book_ticker(stream, payload, id.symbol, recv_ns),
        StreamKind::Trade => normalize_trade(stream, payload, id.symbol),
    }
}

/// Convert our nanosecond ingest clock to the millisecond [`Timestamp`] the
/// domain speaks.
///
/// # Errors
///
/// Refuses a negative `recv_ns`: a pre-epoch ingest time means the clock we were
/// handed is wrong, and stamping events with it would corrupt a recording that
/// later looks perfectly well-formed.
pub fn ingest_ms(recv_ns: i64) -> Result<Timestamp, NormalizeError> {
    if recv_ns < 0 {
        return Err(NormalizeError::InvalidIngestTime { recv_ns });
    }
    Ok(recv_ns / 1_000_000)
}

/// `@bookTicker`: `{"u":..,"s":..,"b":..,"B":..,"a":..,"A":..}`.
///
/// Note what is *not* in that payload: a timestamp. Binance's individual book
/// ticker stream carries no event time at all, so the only honest time we have
/// is our own ingest time. That is why `normalize` takes `recv_ns`, and why it
/// stays a parameter rather than a clock read - a replay must reproduce the same
/// timestamps from the recording, not from the machine replaying it.
fn normalize_book_ticker(
    stream: &str,
    payload: &Value,
    symbol: Symbol,
    recv_ns: i64,
) -> Result<Normalized, NormalizeError> {
    let event = MarketEvent::BookTicker(BookTicker {
        symbol,
        bid: Price(positive(stream, "b", decimal_field(stream, payload, "b")?)?),
        bid_qty: Qty(non_negative(
            stream,
            "B",
            decimal_field(stream, payload, "B")?,
        )?),
        ask: Price(positive(stream, "a", decimal_field(stream, payload, "a")?)?),
        ask_qty: Qty(non_negative(
            stream,
            "A",
            decimal_field(stream, payload, "A")?,
        )?),
        event_time: ingest_ms(recv_ns)?,
    });

    Ok(Normalized {
        event,
        seq: Seq {
            // `u` is the order book updateId: monotonic, but not contiguous.
            id: i64_field(stream, payload, "u")?,
            policy: StreamKind::BookTicker.seq_policy(),
        },
    })
}

/// `@trade`: `{"e":"trade","E":..,"s":..,"t":..,"p":..,"q":..,"T":..,"m":..,"M":..}`.
///
/// `E` (event time) is used for `event_time`, matching the domain field's name.
/// `T` (trade time) is also present and is typically equal; it is left on the
/// floor here and preserved in the recording, so a later milestone that wants
/// exchange-side latency can recover it without a re-capture.
fn normalize_trade(
    stream: &str,
    payload: &Value,
    symbol: Symbol,
) -> Result<Normalized, NormalizeError> {
    let event_time = i64_field(stream, payload, "E")?;
    if event_time < 0 {
        return Err(NormalizeError::OutOfRange {
            stream: stream.to_owned(),
            field: "E",
            value: event_time.to_string(),
            reason: "event time must not be before the Unix epoch",
        });
    }

    let event = MarketEvent::Trade(Trade {
        symbol,
        price: Price(positive(stream, "p", decimal_field(stream, payload, "p")?)?),
        qty: Qty(positive(stream, "q", decimal_field(stream, payload, "q")?)?),
        event_time,
    });

    Ok(Normalized {
        event,
        seq: Seq {
            // `t` is the per-symbol trade id: contiguous, so gaps are countable.
            id: i64_field(stream, payload, "t")?,
            policy: StreamKind::Trade.seq_policy(),
        },
    })
}

// --- field accessors ---

fn field<'a>(
    stream: &str,
    payload: &'a Value,
    key: &'static str,
) -> Result<&'a Value, NormalizeError> {
    payload
        .get(key)
        .filter(|v| !v.is_null())
        .ok_or_else(|| NormalizeError::MissingField {
            stream: stream.to_owned(),
            field: key,
        })
}

fn str_field<'a>(
    stream: &str,
    payload: &'a Value,
    key: &'static str,
) -> Result<&'a str, NormalizeError> {
    let value = field(stream, payload, key)?;
    value.as_str().ok_or_else(|| NormalizeError::WrongType {
        stream: stream.to_owned(),
        field: key,
        expected: "a string",
        found: type_name(value),
    })
}

fn i64_field(stream: &str, payload: &Value, key: &'static str) -> Result<i64, NormalizeError> {
    let value = field(stream, payload, key)?;
    // `as_i64` is exact: it returns `None` for a fractional or out-of-range JSON
    // number rather than rounding one into an integer.
    value.as_i64().ok_or_else(|| NormalizeError::WrongType {
        stream: stream.to_owned(),
        field: key,
        expected: "an integer",
        found: type_name(value),
    })
}

/// Parse a Binance numeric field straight to [`Decimal`].
///
/// The field must be a JSON **string**, which is what Binance sends. A JSON
/// number is refused rather than accepted, and the refusal is the point: a
/// non-integral JSON number lives in `serde_json` as an `f64`, so reading one
/// would put a float on the path between the exchange and an order. There is no
/// `as_f64` anywhere in this crate, and this check is what keeps it that way
/// even if Binance changes the wire format under us.
fn decimal_field(
    stream: &str,
    payload: &Value,
    key: &'static str,
) -> Result<Decimal, NormalizeError> {
    let raw = field(stream, payload, key)?;
    let text = raw.as_str().ok_or_else(|| NormalizeError::WrongType {
        stream: stream.to_owned(),
        field: key,
        expected: "a decimal string (Binance sends numbers as strings; a JSON \
                   number could only be read through a float)",
        found: type_name(raw),
    })?;

    // `from_str_exact`, not `from_str`: the latter silently rounds anything past
    // 28 significant digits. We would rather drop a message than trade on a
    // number that is quietly not the one the exchange sent.
    Decimal::from_str_exact(text).map_err(|_| NormalizeError::NotADecimal {
        stream: stream.to_owned(),
        field: key,
        value: text.to_owned(),
    })
}

fn positive(stream: &str, field: &'static str, value: Decimal) -> Result<Decimal, NormalizeError> {
    if value.is_sign_negative() || value.is_zero() {
        return Err(NormalizeError::OutOfRange {
            stream: stream.to_owned(),
            field,
            value: value.to_string(),
            reason: "must be strictly positive",
        });
    }
    Ok(value)
}

fn non_negative(
    stream: &str,
    field: &'static str,
    value: Decimal,
) -> Result<Decimal, NormalizeError> {
    if value.is_sign_negative() && !value.is_zero() {
        return Err(NormalizeError::OutOfRange {
            stream: stream.to_owned(),
            field,
            value: value.to_string(),
            reason: "must not be negative",
        });
    }
    Ok(value)
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const BOOK: &str = "btcusdt@bookTicker";
    const TRADE: &str = "btcusdt@trade";
    /// 2024-04-05T18:14:38.901234567Z, in nanoseconds.
    const RECV_NS: i64 = 1_712_340_878_901_234_567;

    fn book_payload() -> Value {
        json!({
            "u": 34_181_873,
            "s": "BTCUSDT",
            "b": "64999.99000000",
            "B": "0.03984000",
            "a": "65000.00000000",
            "A": "1.42150000"
        })
    }

    fn trade_payload() -> Value {
        json!({
            "e": "trade",
            "E": 1_712_340_878_901_i64,
            "s": "BTCUSDT",
            "t": 3_729_481,
            "p": "65000.01000000",
            "q": "0.00099000",
            "T": 1_712_340_878_900_i64,
            "m": true,
            "M": true
        })
    }

    fn book_ticker(event: MarketEvent) -> BookTicker {
        match event {
            MarketEvent::BookTicker(b) => b,
            other => panic!("expected a book ticker, got {other:?}"),
        }
    }

    fn trade(event: MarketEvent) -> Trade {
        match event {
            MarketEvent::Trade(t) => t,
            other => panic!("expected a trade, got {other:?}"),
        }
    }

    fn dec(s: &str) -> Decimal {
        Decimal::from_str_exact(s).expect("test literal should be an exact decimal")
    }

    // --- purity ---

    #[test]
    fn normalization_is_a_pure_function_of_its_arguments() {
        // Same inputs, many times, interleaved across streams: identical outputs.
        // This is the property the whole replay story rests on.
        let first = normalize(BOOK, &book_payload(), RECV_NS).expect("valid");
        let first_trade = normalize(TRADE, &trade_payload(), RECV_NS).expect("valid");
        for _ in 0..100 {
            assert_eq!(
                normalize(BOOK, &book_payload(), RECV_NS).expect("valid"),
                first
            );
            assert_eq!(
                normalize(TRADE, &trade_payload(), RECV_NS).expect("valid"),
                first_trade
            );
        }
    }

    #[test]
    fn the_ingest_time_is_an_argument_not_a_clock() {
        // Changing only `recv_ns` changes only the book ticker's timestamp, and
        // does not touch a trade at all (which carries its own event time).
        let a = normalize(BOOK, &book_payload(), 1_000_000_000).expect("valid");
        let b = normalize(BOOK, &book_payload(), 2_000_000_000).expect("valid");
        assert_eq!(book_ticker(a.event).event_time, 1_000);
        assert_eq!(book_ticker(b.event).event_time, 2_000);

        let t1 = normalize(TRADE, &trade_payload(), 1_000_000_000).expect("valid");
        let t2 = normalize(TRADE, &trade_payload(), i64::MAX).expect("valid");
        assert_eq!(t1, t2, "a trade's event time comes from the payload alone");
    }

    #[test]
    fn a_trade_takes_its_event_time_and_numbers_from_the_payload() {
        let t = trade(
            normalize(TRADE, &trade_payload(), RECV_NS)
                .expect("valid")
                .event,
        );
        assert_eq!(t.symbol.as_str(), "BTCUSDT");
        assert_eq!(t.price.0, dec("65000.01000000"));
        assert_eq!(t.qty.0, dec("0.00099000"));
        // `E` (event time), not `T` (trade time) and not our ingest clock.
        assert_eq!(t.event_time, 1_712_340_878_901);
    }

    // --- decimals ---

    #[test]
    fn a_value_that_cannot_survive_f64_round_trips_exactly() {
        // 21 significant digits. As an f64 this becomes 1.0 exactly; if any
        // float ever creeps into the parse path, this test goes red.
        let exact = "1.00000000000000000001";
        let payload = json!({
            "u": 1, "s": "BTCUSDT",
            "b": exact, "B": "1", "a": "2", "A": "1"
        });
        let bid = book_ticker(normalize(BOOK, &payload, RECV_NS).expect("valid").event).bid;
        assert_eq!(bid.0, dec(exact));
        assert_eq!(
            bid.0.to_string(),
            exact,
            "scale and digits must be preserved"
        );
        assert_ne!(
            bid.0,
            Decimal::ONE,
            "an f64 round trip would collapse this to 1"
        );
    }

    #[test]
    fn trailing_zeros_and_scale_are_preserved_verbatim() {
        // Decimal equality ignores scale, so equality alone would not catch a
        // parse that dropped it. Assert the rendered form and the scale too.
        let b = book_ticker(
            normalize(BOOK, &book_payload(), RECV_NS)
                .expect("valid")
                .event,
        );
        assert_eq!(b.bid.0.to_string(), "64999.99000000");
        assert_eq!(b.bid.0.scale(), 8);
        assert_eq!(b.ask_qty.0.to_string(), "1.42150000");
        assert_eq!(b.ask_qty.0.scale(), 8);
    }

    #[test]
    fn a_numeric_field_sent_as_a_json_number_is_refused() {
        // The f64 gate. `serde_json` holds a fractional JSON number as an f64,
        // so reading one would put a float between the exchange and an order.
        for bad in [json!(64_999.99), json!(65_000), json!(1e5)] {
            let mut payload = book_payload();
            payload["b"] = bad.clone();
            let err = normalize(BOOK, &payload, RECV_NS).expect_err("must refuse a JSON number");
            assert!(
                matches!(
                    &err,
                    NormalizeError::WrongType {
                        field: "b",
                        found: "a number",
                        ..
                    }
                ),
                "got {err:?} for {bad}"
            );
            assert!(err.to_string().contains("float"), "{err}");
        }
    }

    #[test]
    fn a_decimal_we_cannot_represent_exactly_is_dropped_not_rounded() {
        // 30 significant digits: past Decimal's 28. `from_str` would round this
        // silently; `from_str_exact` refuses, and so do we.
        let mut payload = book_payload();
        payload["b"] = json!("1.00000000000000000000000000001");
        let err = normalize(BOOK, &payload, RECV_NS).expect_err("must refuse");
        assert!(
            matches!(&err, NormalizeError::NotADecimal { field: "b", .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn non_numeric_decimal_strings_are_refused() {
        for bad in [
            "",
            " ",
            "abc",
            "1.2.3",
            "NaN",
            "Infinity",
            "-Infinity",
            "1e400",
            "0x10",
            "1,5",
            "64999.99 ",
            " 64999.99",
            "+-1",
            "--1",
        ] {
            let mut payload = book_payload();
            payload["b"] = json!(bad);
            let err = normalize(BOOK, &payload, RECV_NS)
                .map(|n| n.event)
                .expect_err(&format!("`{bad}` must not parse as a price"));
            assert!(
                matches!(&err, NormalizeError::NotADecimal { field: "b", .. })
                    || matches!(&err, NormalizeError::OutOfRange { field: "b", .. }),
                "`{bad}` gave {err:?}"
            );
        }
    }

    // --- range checks ---

    #[test]
    fn non_positive_prices_are_refused() {
        for (field, value) in [("b", "0"), ("b", "-1"), ("a", "0"), ("a", "-0.5")] {
            let mut payload = book_payload();
            payload[field] = json!(value);
            let err = normalize(BOOK, &payload, RECV_NS).expect_err("must refuse");
            assert!(
                matches!(&err, NormalizeError::OutOfRange { field: f, .. } if *f == field),
                "got {err:?} for {field}={value}"
            );
        }
        let mut payload = trade_payload();
        payload["p"] = json!("0");
        assert!(matches!(
            normalize(TRADE, &payload, RECV_NS).expect_err("must refuse"),
            NormalizeError::OutOfRange { field: "p", .. }
        ));
    }

    #[test]
    fn negative_quantities_are_refused_but_an_empty_side_is_allowed() {
        let mut payload = book_payload();
        payload["B"] = json!("-1");
        assert!(matches!(
            normalize(BOOK, &payload, RECV_NS).expect_err("must refuse"),
            NormalizeError::OutOfRange { field: "B", .. }
        ));

        // A zero book side is thin, not corrupt. Normalization reports what the
        // exchange said; judging it is the risk layer's job, not this one's.
        let mut payload = book_payload();
        payload["B"] = json!("0.00000000");
        let b = book_ticker(normalize(BOOK, &payload, RECV_NS).expect("valid").event);
        assert_eq!(b.bid_qty.0, Decimal::ZERO);
    }

    #[test]
    fn a_zero_size_trade_is_refused() {
        // Unlike a book side, a trade of zero size is not a thin market - it is
        // a message we do not understand.
        let mut payload = trade_payload();
        payload["q"] = json!("0");
        assert!(matches!(
            normalize(TRADE, &payload, RECV_NS).expect_err("must refuse"),
            NormalizeError::OutOfRange { field: "q", .. }
        ));
    }

    #[test]
    fn a_negative_ingest_time_is_refused() {
        assert_eq!(
            normalize(BOOK, &book_payload(), -1).expect_err("must refuse"),
            NormalizeError::InvalidIngestTime { recv_ns: -1 }
        );
        assert_eq!(
            ingest_ms(-1).expect_err("must refuse"),
            NormalizeError::InvalidIngestTime { recv_ns: -1 }
        );
        assert_eq!(ingest_ms(0).expect("epoch is fine"), 0);
        assert_eq!(
            ingest_ms(1_999_999).expect("valid"),
            1,
            "truncates, never rounds up"
        );
    }

    #[test]
    fn a_negative_event_time_is_refused() {
        let mut payload = trade_payload();
        payload["E"] = json!(-1);
        assert!(matches!(
            normalize(TRADE, &payload, RECV_NS).expect_err("must refuse"),
            NormalizeError::OutOfRange { field: "E", .. }
        ));
    }

    // --- cross-checks ---

    #[test]
    fn a_payload_for_the_wrong_symbol_is_refused() {
        // The failure this prevents shows up much later as an inexplicable
        // position, so it is worth refusing loudly right here.
        let mut payload = book_payload();
        payload["s"] = json!("ETHUSDT");
        assert_eq!(
            normalize(BOOK, &payload, RECV_NS).expect_err("must refuse"),
            NormalizeError::SymbolMismatch {
                stream: BOOK.to_owned(),
                stream_symbol: "BTCUSDT".to_owned(),
                payload_symbol: "ETHUSDT".to_owned(),
            }
        );
    }

    #[test]
    fn the_payload_symbol_may_differ_only_in_case() {
        let mut payload = book_payload();
        payload["s"] = json!("btcusdt");
        let b = book_ticker(normalize(BOOK, &payload, RECV_NS).expect("valid").event);
        assert_eq!(b.symbol.as_str(), "BTCUSDT", "canonicalised to uppercase");
    }

    #[test]
    fn a_payload_whose_event_type_disagrees_with_its_stream_is_refused() {
        let mut payload = trade_payload();
        payload["e"] = json!("aggTrade");
        assert_eq!(
            normalize(TRADE, &payload, RECV_NS).expect_err("must refuse"),
            NormalizeError::EventTypeMismatch {
                stream: TRADE.to_owned(),
                expected: "trade",
                found: "aggTrade".to_owned(),
            }
        );
    }

    #[test]
    fn a_book_ticker_payload_delivered_on_a_trade_stream_is_refused() {
        // Realistic mix-up: the frames are similar and both carry `s`.
        let err = normalize(TRADE, &book_payload(), RECV_NS).expect_err("must refuse");
        assert!(
            matches!(&err, NormalizeError::MissingField { field: "e", .. }),
            "got {err:?}"
        );
    }

    // --- structure ---

    #[test]
    fn missing_fields_are_named_in_the_error() {
        for field in ["u", "s", "b", "B", "a", "A"] {
            let mut payload = book_payload();
            payload.as_object_mut().expect("object").remove(field);
            let err = normalize(BOOK, &payload, RECV_NS).expect_err("must refuse");
            assert!(
                matches!(&err, NormalizeError::MissingField { field: f, .. } if *f == field)
                    || matches!(&err, NormalizeError::WrongType { field: f, .. } if *f == field),
                "removing `{field}` gave {err:?}"
            );
            assert!(err.to_string().contains(field), "{err}");
        }
        for field in ["e", "E", "s", "t", "p", "q"] {
            let mut payload = trade_payload();
            payload.as_object_mut().expect("object").remove(field);
            let err = normalize(TRADE, &payload, RECV_NS).expect_err("must refuse");
            assert!(err.to_string().contains(field), "removing `{field}`: {err}");
        }
    }

    #[test]
    fn a_null_field_counts_as_missing_rather_than_as_a_value() {
        let mut payload = book_payload();
        payload["b"] = Value::Null;
        assert!(matches!(
            normalize(BOOK, &payload, RECV_NS).expect_err("must refuse"),
            NormalizeError::MissingField { field: "b", .. }
        ));
    }

    #[test]
    fn a_payload_that_is_not_an_object_is_refused() {
        for (payload, found) in [
            (json!([1, 2, 3]), "an array"),
            (json!("btcusdt"), "a string"),
            (json!(7), "a number"),
            (Value::Null, "null"),
        ] {
            assert_eq!(
                normalize(BOOK, &payload, RECV_NS).expect_err("must refuse"),
                NormalizeError::NotAnObject {
                    stream: BOOK.to_owned(),
                    found,
                }
            );
        }
    }

    #[test]
    fn unknown_extra_fields_are_ignored_not_fatal() {
        // Binance adds fields over time. Adding one must not stop the feed.
        let mut payload = book_payload();
        payload["someNewFieldBinanceAddedIn2027"] = json!("whatever");
        assert!(normalize(BOOK, &payload, RECV_NS).is_ok());
    }

    #[test]
    fn a_fractional_sequence_id_is_refused_rather_than_truncated() {
        let mut payload = book_payload();
        payload["u"] = json!(34_181_873.5);
        assert!(matches!(
            normalize(BOOK, &payload, RECV_NS).expect_err("must refuse"),
            NormalizeError::WrongType { field: "u", .. }
        ));
    }

    // --- sequence ids ---

    #[test]
    fn the_book_ticker_sequence_is_the_update_id_and_only_monotonic() {
        let n = normalize(BOOK, &book_payload(), RECV_NS).expect("valid");
        assert_eq!(
            n.seq,
            Seq {
                id: 34_181_873,
                policy: SeqPolicy::Monotonic
            }
        );
    }

    #[test]
    fn the_trade_sequence_is_the_trade_id_and_is_contiguous() {
        let n = normalize(TRADE, &trade_payload(), RECV_NS).expect("valid");
        assert_eq!(
            n.seq,
            Seq {
                id: 3_729_481,
                policy: SeqPolicy::Contiguous
            }
        );
    }

    // --- stream names ---

    #[test]
    fn stream_names_round_trip_through_parse_and_build() {
        for (name, symbol, kind) in [
            ("btcusdt@bookTicker", "BTCUSDT", StreamKind::BookTicker),
            ("btcusdt@trade", "BTCUSDT", StreamKind::Trade),
            ("1inchusdt@trade", "1INCHUSDT", StreamKind::Trade),
        ] {
            let id = parse_stream(name).expect("valid stream name");
            assert_eq!(id.symbol.as_str(), symbol);
            assert_eq!(id.kind, kind);
            assert_eq!(kind.stream_name(&id.symbol), name, "must rebuild verbatim");
        }
    }

    #[test]
    fn stream_kind_suffixes_use_binances_exact_spelling() {
        // Binance is case-sensitive here and `bookticker` is silently ignored.
        assert_eq!(StreamKind::BookTicker.suffix(), "bookTicker");
        assert_eq!(StreamKind::Trade.suffix(), "trade");
        assert!(parse_stream("btcusdt@bookticker").is_err());
    }

    #[test]
    fn unsupported_and_malformed_stream_names_are_refused() {
        for name in [
            "btcusdt@aggTrade",
            "btcusdt@depth20@100ms",
            "btcusdt@kline_1m",
        ] {
            let err = parse_stream(name).expect_err("must refuse");
            assert!(
                matches!(err, NormalizeError::UnsupportedStreamKind { .. }),
                "{name}: {err:?}"
            );
        }
        for name in ["btcusdt", "", "@trade", "btcusdt@", "@"] {
            assert!(
                matches!(
                    parse_stream(name).expect_err("must refuse"),
                    NormalizeError::MalformedStreamName { .. }
                ),
                "{name}"
            );
        }
        for name in ["btc-usdt@trade", "btc usdt@trade", "btc/usdt@trade"] {
            assert!(
                matches!(
                    parse_stream(name).expect_err("must refuse"),
                    NormalizeError::InvalidSymbol { .. }
                ),
                "{name}"
            );
        }
    }

    #[test]
    fn stream_kind_metadata_is_consistent() {
        assert_eq!(StreamKind::ALL.len(), 2);
        assert_eq!(StreamKind::BookTicker.event_type(), None);
        assert_eq!(StreamKind::Trade.event_type(), Some("trade"));
        for kind in StreamKind::ALL {
            assert_eq!(StreamKind::from_suffix(kind.suffix()), Some(*kind));
        }
    }
}
