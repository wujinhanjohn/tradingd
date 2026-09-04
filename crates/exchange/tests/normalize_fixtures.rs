//! Fixture tests for the pure normalization function.
//!
//! Every case is driven from a file in `tests/fixtures/`, holding a raw frame
//! exactly as the Binance combined-stream endpoint sends it. Nothing here
//! touches the network, a clock, or the live testnet: the fixtures are the
//! captured bytes, and `exchange::normalize` is a pure function of them.
//!
//! Two things are asserted, not one. The whole `domain::MarketEvent` must come
//! out equal to the expected value - and, separately, every `Decimal` must
//! render back to the exact string Binance sent. `Decimal`'s `PartialEq` ignores
//! scale (`1.50 == 1.5`), so equality alone would not notice a parse that
//! dropped trailing zeros or otherwise changed the number's precision.

use std::path::PathBuf;

use domain::{BookTicker, Decimal, MarketEvent, Price, Qty, Symbol, Trade};
use exchange::{normalize, Seq, SeqPolicy};
use serde_json::Value;

/// 2024-04-05T18:14:38.901234567Z. Fixed, so the tests are deterministic.
const RECV_NS: i64 = 1_712_340_878_901_234_567;

/// Read a captured frame and split it into its stream name and raw payload.
///
/// Combined-stream frames are wrapped as `{"stream":..,"data":..}`; the `data`
/// object is what gets recorded and what `normalize` is handed, unchanged.
fn fixture(name: &str) -> (String, Value) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading fixture {}: {e}", path.display()));
    let frame: Value = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()));

    let stream = frame["stream"]
        .as_str()
        .unwrap_or_else(|| panic!("fixture {} has no `stream`", path.display()))
        .to_owned();
    let payload = frame["data"].clone();
    assert!(
        payload.is_object(),
        "fixture {} has no `data` object",
        path.display()
    );
    (stream, payload)
}

fn sym(s: &str) -> Symbol {
    Symbol::new(s).expect("fixture symbol should be valid")
}

fn dec(s: &str) -> Decimal {
    Decimal::from_str_exact(s).expect("expected value should be an exact decimal")
}

/// Assert a decimal equals the string Binance sent *and* still renders as it.
///
/// The second half is the one that catches a lossy parse: `Decimal` compares
/// equal across scales, so `assert_eq!` alone would pass on a value that had
/// quietly lost its precision.
fn assert_exact(actual: Decimal, sent: &str) {
    assert_eq!(
        actual,
        dec(sent),
        "value differs from the string Binance sent"
    );
    assert_eq!(
        actual.to_string(),
        sent,
        "decimal did not round-trip to the exact string Binance sent"
    );
}

#[test]
fn book_ticker_btcusdt_normalizes_to_the_exact_domain_event() {
    let (stream, payload) = fixture("book_ticker_btcusdt.json");
    let out = normalize(&stream, &payload, RECV_NS).expect("fixture should normalize");

    assert_eq!(
        out.event,
        MarketEvent::BookTicker(BookTicker {
            symbol: sym("BTCUSDT"),
            bid: Price(dec("64999.99000000")),
            bid_qty: Qty(dec("0.03984000")),
            ask: Price(dec("65000.00000000")),
            ask_qty: Qty(dec("1.42150000")),
            // `@bookTicker` carries no exchange timestamp, so the event time is
            // our ingest clock, truncated from nanoseconds to milliseconds.
            event_time: 1_712_340_878_901,
        })
    );

    let MarketEvent::BookTicker(b) = out.event else {
        panic!("expected a book ticker");
    };
    assert_exact(b.bid.0, "64999.99000000");
    assert_exact(b.bid_qty.0, "0.03984000");
    assert_exact(b.ask.0, "65000.00000000");
    assert_exact(b.ask_qty.0, "1.42150000");

    assert_eq!(
        out.seq,
        Seq {
            id: 34_181_873,
            policy: SeqPolicy::Monotonic,
        }
    );
}

#[test]
fn book_ticker_ethusdt_normalizes_to_the_exact_domain_event() {
    let (stream, payload) = fixture("book_ticker_ethusdt.json");
    let out = normalize(&stream, &payload, RECV_NS).expect("fixture should normalize");

    assert_eq!(
        out.event,
        MarketEvent::BookTicker(BookTicker {
            symbol: sym("ETHUSDT"),
            bid: Price(dec("3421.07000000")),
            bid_qty: Qty(dec("12.50900000")),
            ask: Price(dec("3421.08000000")),
            ask_qty: Qty(dec("3.00000000")),
            event_time: 1_712_340_878_901,
        })
    );
    // An updateId past 2^32, to prove nothing narrows it to 32 bits.
    assert_eq!(out.seq.id, 9_182_736_455);
}

#[test]
fn a_sub_cent_price_keeps_every_significant_digit() {
    // The case a float would mangle first: eight decimal places on a price whose
    // significant digits start well after the point.
    let (stream, payload) = fixture("book_ticker_tiny_price.json");
    let out = normalize(&stream, &payload, RECV_NS).expect("fixture should normalize");

    let MarketEvent::BookTicker(b) = out.event else {
        panic!("expected a book ticker");
    };
    assert_eq!(b.symbol.as_str(), "1000SATSUSDT");
    assert_exact(b.bid.0, "0.00000123");
    assert_exact(b.ask.0, "0.00000124");
    assert_exact(b.bid_qty.0, "98765432.10000000");
    assert_exact(b.ask_qty.0, "12345678.90000000");

    // The spread is one tick, exactly. Computed in Decimal; a float would give
    // something like 1.0000000000287557e-8 here.
    assert_eq!(b.ask.0 - b.bid.0, dec("0.00000001"));
}

#[test]
fn trade_btcusdt_normalizes_to_the_exact_domain_event() {
    let (stream, payload) = fixture("trade_btcusdt_buyer_maker.json");
    let out = normalize(&stream, &payload, RECV_NS).expect("fixture should normalize");

    assert_eq!(
        out.event,
        MarketEvent::Trade(Trade {
            symbol: sym("BTCUSDT"),
            price: Price(dec("65000.01000000")),
            qty: Qty(dec("0.00099000")),
            // `E` from the payload, not our ingest clock: passing a wildly
            // different `recv_ns` below must not move this.
            event_time: 1_712_340_878_901,
        })
    );

    let MarketEvent::Trade(t) = out.event else {
        panic!("expected a trade");
    };
    assert_exact(t.price.0, "65000.01000000");
    assert_exact(t.qty.0, "0.00099000");

    assert_eq!(
        out.seq,
        Seq {
            id: 3_729_481,
            policy: SeqPolicy::Contiguous,
        }
    );

    let elsewhere = normalize(&stream, &payload, 0).expect("valid");
    assert_eq!(
        elsewhere.event,
        MarketEvent::Trade(t),
        "a trade must not depend on the ingest clock at all"
    );
    assert_eq!(elsewhere.seq, out.seq);
}

#[test]
fn trade_ethusdt_normalizes_to_the_exact_domain_event() {
    let (stream, payload) = fixture("trade_ethusdt.json");
    let out = normalize(&stream, &payload, RECV_NS).expect("fixture should normalize");

    assert_eq!(
        out.event,
        MarketEvent::Trade(Trade {
            symbol: sym("ETHUSDT"),
            price: Price(dec("3421.08000000")),
            qty: Qty(dec("0.03500000")),
            event_time: 1_712_340_879_012,
        })
    );
    assert_eq!(out.seq.id, 881_230_044);
}

#[test]
fn consecutive_trades_carry_consecutive_ids() {
    // Pins the contiguity claim that gap detection is built on: `t` increments
    // by exactly one between two trades captured back to back.
    let (_, first) = fixture("trade_btcusdt_buyer_maker.json");
    let (stream, second) = fixture("trade_btcusdt_next.json");

    let a = normalize(&stream, &first, RECV_NS).expect("valid");
    let b = normalize(&stream, &second, RECV_NS).expect("valid");

    assert_eq!(b.seq.id, a.seq.id + 1);
    assert_eq!(a.seq.policy, SeqPolicy::Contiguous);
}

#[test]
fn every_fixture_normalizes_and_does_so_identically_every_time() {
    // Sweeps the directory rather than a hand-written list, so a fixture added
    // later cannot sit unexercised. Also re-asserts purity across all of them.
    //
    // Only the top level: captured stream frames live here, and the
    // `exchangeInfo` captures - a different payload shape entirely, for a
    // different parse - live in `fixtures/exchange_info/`.
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut checked = 0;

    for entry in std::fs::read_dir(&dir).expect("fixtures directory should exist") {
        let path = entry.expect("readable dir entry").path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .expect("fixture file name");

        let (stream, payload) = fixture(name);
        let first = normalize(&stream, &payload, RECV_NS)
            .unwrap_or_else(|e| panic!("fixture {name} should normalize: {e}"));
        for _ in 0..10 {
            assert_eq!(
                normalize(&stream, &payload, RECV_NS).expect("valid"),
                first,
                "fixture {name} did not normalize identically on a repeat run"
            );
        }
        checked += 1;
    }

    assert!(
        checked >= 6,
        "expected the captured fixtures, found {checked}"
    );
}
