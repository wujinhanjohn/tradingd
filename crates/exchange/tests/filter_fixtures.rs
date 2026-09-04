//! Fixture tests for the pure `exchangeInfo` parse.
//!
//! Driven from files in `tests/fixtures/`, the first of which is a verbatim
//! capture of the live testnet response. Nothing here touches the network: the
//! fixture is the bytes the exchange actually sent, and `parse_exchange_info` is
//! a pure function of them.
//!
//! Two things are asserted throughout, not one. The parsed `SymbolFilters` must
//! be *equal* to the expected value - and, separately, every `Decimal` must
//! render back to the exact string Binance sent. `Decimal`'s `PartialEq` ignores
//! scale (`1.50 == 1.5`), so equality alone would not notice a parse that
//! dropped precision, and the precision is what the wire format is checked
//! against.

use std::path::PathBuf;

use domain::{Decimal, Price, Qty, Symbol};
use exchange::{parse_exchange_info, FilterParseError, LotSize, STATUS_TRADING};
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/exchange_info")
        .join(name);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading fixture {}: {e}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()))
}

fn sym(name: &str) -> Symbol {
    Symbol::new(name).expect("fixture symbol should be valid")
}

fn dec(text: &str) -> Decimal {
    Decimal::from_str_exact(text).expect("expected value should be an exact decimal")
}

/// Assert a decimal equals the string Binance sent *and* still renders as it.
fn assert_exact(actual: Decimal, expected: &str) {
    assert_eq!(actual, dec(expected), "value");
    assert_eq!(actual.to_string(), expected, "scale and rendering");
}

#[test]
fn the_real_testnet_btcusdt_response_parses_to_exact_filters() {
    let payload = fixture("btcusdt.json");
    let parsed = parse_exchange_info(&payload, &[sym("BTCUSDT")]).expect("must parse");
    assert_eq!(parsed.len(), 1);
    let info = &parsed[0];

    assert_eq!(info.status, STATUS_TRADING);
    assert_eq!(info.filters.symbol(), &sym("BTCUSDT"));

    assert_exact(info.filters.tick_size().0, "0.01000000");
    assert_exact(info.filters.min_price().0, "0.01000000");
    assert_exact(info.filters.max_price().0, "1000000.00000000");
    assert_exact(info.filters.step_size().0, "0.00001000");
    assert_exact(info.filters.min_qty().0, "0.00001000");
    assert_exact(info.filters.max_qty().0, "9000.00000000");
    // NOTIONAL, not the legacy MIN_NOTIONAL - confirmed against the capture.
    assert_exact(info.filters.min_notional(), "5.00000000");
}

#[test]
fn market_lot_size_is_carried_for_milestone_six_but_not_enforced_now() {
    let payload = fixture("btcusdt.json");
    let parsed = parse_exchange_info(&payload, &[sym("BTCUSDT")]).expect("must parse");

    // Note the zero step and zero minQty: Binance's own spelling for a disabled
    // rule, carried through rather than refused.
    assert_eq!(
        parsed[0].market_lot_size,
        Some(LotSize {
            min_qty: Qty(dec("0.00000000")),
            max_qty: Qty(dec("141.67845966")),
            step_size: Qty(dec("0.00000000")),
        })
    );
}

#[test]
fn every_unmodelled_filter_type_is_surfaced_rather_than_dropped() {
    // These are rules the exchange will enforce and the quantizer will not. If
    // they were dropped silently, milestone 6 would meet them as an inexplicable
    // order rejection instead of as a line in the startup log.
    let payload = fixture("btcusdt.json");
    let parsed = parse_exchange_info(&payload, &[sym("BTCUSDT")]).expect("must parse");

    assert_eq!(
        parsed[0].unmodeled,
        vec![
            "ICEBERG_PARTS".to_owned(),
            "MAX_NUM_ALGO_ORDERS".to_owned(),
            "MAX_NUM_ORDERS".to_owned(),
            "MAX_NUM_ORDER_AMENDS".to_owned(),
            "MAX_NUM_ORDER_LISTS".to_owned(),
            "PERCENT_PRICE_BY_SIDE".to_owned(),
            "TRAILING_DELTA".to_owned(),
        ],
        "the unmodelled types must be listed, sorted, in full"
    );
}

#[test]
fn a_filter_type_we_have_never_heard_of_is_surfaced_and_does_not_corrupt_the_parse() {
    // The fail-open-on-additive-change boundary: a *new field* inside a filter we
    // model is ignored, because Binance adding one must not stop a running bot.
    // A *new filter type* is a new rule, and new rules get said out loud.
    let payload = fixture("future_fields.json");
    let parsed = parse_exchange_info(&payload, &[sym("BTCUSDT")]).expect("must still parse");

    assert!(
        parsed[0]
            .unmodeled
            .contains(&"SOME_NEW_RULE_2027".to_owned()),
        "the unknown rule must be surfaced: {:?}",
        parsed[0].unmodeled
    );
    // And the modelled filters came through untouched, additive field and all.
    assert_exact(parsed[0].filters.tick_size().0, "0.01000000");
    assert_exact(parsed[0].filters.min_notional(), "5.00000000");
}

#[test]
fn the_legacy_min_notional_filter_is_accepted_as_a_fallback() {
    let payload = fixture("two_symbols.json");
    let parsed =
        parse_exchange_info(&payload, &[sym("BTCUSDT"), sym("ETHUSDT")]).expect("must parse");
    assert_eq!(parsed.len(), 2);

    assert_exact(parsed[1].filters.min_notional(), "10.00000000");
    assert_eq!(parsed[1].filters.symbol(), &sym("ETHUSDT"));
    // Status is carried, not judged: a halted symbol is worth a warning, and it
    // is milestone 6's business whether an order may be sent against one.
    assert_eq!(parsed[1].status, "BREAK");
}

#[test]
fn the_result_is_in_the_order_asked_for_not_the_order_the_exchange_listed() {
    let payload = fixture("two_symbols.json");
    let parsed =
        parse_exchange_info(&payload, &[sym("ETHUSDT"), sym("BTCUSDT")]).expect("must parse");
    assert_eq!(parsed[0].filters.symbol(), &sym("ETHUSDT"));
    assert_eq!(parsed[1].filters.symbol(), &sym("BTCUSDT"));
}

#[test]
fn symbols_we_did_not_ask_for_are_ignored() {
    // A full-market response is a valid answer to a narrow question.
    let payload = fixture("two_symbols.json");
    let parsed = parse_exchange_info(&payload, &[sym("ETHUSDT")]).expect("must parse");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].filters.symbol(), &sym("ETHUSDT"));
}

#[test]
fn a_configured_symbol_missing_from_the_response_is_refused_not_skipped() {
    // Fail-closed. Skipping it would leave the bot running with no rules for a
    // symbol it believes it is trading.
    let payload = fixture("btcusdt.json");
    let err =
        parse_exchange_info(&payload, &[sym("BTCUSDT"), sym("ETHUSDT")]).expect_err("must refuse");
    assert_eq!(
        err,
        FilterParseError::SymbolNotListed {
            symbol: "ETHUSDT".to_owned()
        }
    );
    assert!(err.to_string().contains("Refusing to start"), "{err}");
}

#[test]
fn a_bare_json_number_in_a_decimal_field_is_refused() {
    // The banned float path. `serde_json` holds a non-integral JSON number as an
    // f64, so reading one would put binary floating point between the exchange
    // and an order.
    let payload = fixture("bare_number.json");
    let err = parse_exchange_info(&payload, &[sym("BTCUSDT")]).expect_err("must refuse");

    assert_eq!(
        err,
        FilterParseError::NotAString {
            symbol: "BTCUSDT".to_owned(),
            filter_type: "PRICE_FILTER",
            field: "tickSize",
            found: "a number",
        }
    );
    assert!(err.to_string().contains("float"), "{err}");
}

#[test]
fn a_symbol_with_no_lot_size_filter_is_refused() {
    let payload = fixture("missing_lot_size.json");
    let err = parse_exchange_info(&payload, &[sym("BTCUSDT")]).expect_err("must refuse");
    assert_eq!(
        err,
        FilterParseError::MissingFilter {
            symbol: "BTCUSDT".to_owned(),
            filter_type: "LOT_SIZE",
        }
    );
}

#[test]
fn a_malformed_payload_is_refused_rather_than_half_read() {
    for (payload, expected) in [
        (
            serde_json::json!([]),
            FilterParseError::NotAnObject { found: "an array" },
        ),
        (serde_json::json!({}), FilterParseError::MissingSymbols),
        (
            serde_json::json!({"symbols": ["BTCUSDT"]}),
            FilterParseError::SymbolNotAnObject {
                index: 0,
                found: "a string",
            },
        ),
        (
            serde_json::json!({"symbols": [{"status": "TRADING", "filters": []}]}),
            FilterParseError::MissingField {
                index: 0,
                field: "symbol",
            },
        ),
        (
            serde_json::json!({"symbols": [{"symbol": "btcusdt", "status": "TRADING", "filters": []}]}),
            FilterParseError::InvalidSymbol {
                index: 0,
                symbol: "btcusdt".to_owned(),
            },
        ),
    ] {
        assert_eq!(
            parse_exchange_info(&payload, &[sym("BTCUSDT")]).expect_err("must refuse"),
            expected
        );
    }
}

#[test]
fn a_repeated_symbol_or_filter_is_refused_rather_than_arbitrated() {
    let one = fixture("btcusdt.json");
    let entry = one["symbols"][0].clone();

    let doubled = serde_json::json!({"symbols": [entry.clone(), entry.clone()]});
    assert_eq!(
        parse_exchange_info(&doubled, &[sym("BTCUSDT")]).expect_err("must refuse"),
        FilterParseError::DuplicateSymbol {
            symbol: "BTCUSDT".to_owned()
        }
    );

    let mut twice = entry;
    let filters = twice["filters"].as_array().expect("filters").clone();
    let price_filter = filters
        .iter()
        .find(|f| f["filterType"] == "PRICE_FILTER")
        .expect("a price filter")
        .clone();
    twice["filters"]
        .as_array_mut()
        .expect("filters")
        .push(price_filter);
    let payload = serde_json::json!({"symbols": [twice]});
    assert_eq!(
        parse_exchange_info(&payload, &[sym("BTCUSDT")]).expect_err("must refuse"),
        FilterParseError::DuplicateFilter {
            symbol: "BTCUSDT".to_owned(),
            filter_type: "PRICE_FILTER".to_owned(),
        }
    );
}

#[test]
fn filters_that_no_order_could_satisfy_are_refused_at_the_parse() {
    // An inverted range would otherwise become a symbol that silently rejects
    // every order at quantize time, which looks like a bug in the strategy.
    let mut payload = fixture("btcusdt.json");
    for filter in payload["symbols"][0]["filters"]
        .as_array_mut()
        .expect("filters")
    {
        if filter["filterType"] == "PRICE_FILTER" {
            filter["minPrice"] = serde_json::json!("2000000.00000000");
        }
    }

    let err = parse_exchange_info(&payload, &[sym("BTCUSDT")]).expect_err("must refuse");
    assert!(
        matches!(err, FilterParseError::Unusable { .. }),
        "expected an unusable-filters error, got {err:?}"
    );
    assert!(err.to_string().contains("BTCUSDT"), "{err}");
}

#[test]
fn the_parsed_filters_are_what_the_quantizer_then_enforces() {
    // The seam this whole milestone exists to join: parse -> quantize, with the
    // real captured filters in between and no hand-written values anywhere.
    let payload = fixture("btcusdt.json");
    let parsed = parse_exchange_info(&payload, &[sym("BTCUSDT")]).expect("must parse");
    let filters = &parsed[0].filters;

    let intent = domain::OrderIntent {
        client_order_id: domain::ClientOrderId("fixture-1".to_owned()),
        symbol: sym("BTCUSDT"),
        side: domain::Side::Buy,
        kind: domain::OrderKind::Limit {
            price: Price(dec("65000.126")),
        },
        qty: Qty(dec("0.0012345")),
        tif: domain::TimeInForce::Gtc,
    };

    let order = domain::quantize(&intent, filters).expect("a plausible order must quantize");
    assert_eq!(order.price().0.to_string(), "65000.12000000");
    assert_eq!(order.qty().0.to_string(), "0.00123000");

    // And one under the real 5 USDT minimum notional is refused with the reason.
    let dust = domain::OrderIntent {
        qty: Qty(dec("0.00001")),
        ..intent
    };
    assert!(matches!(
        domain::quantize(&dust, filters),
        Err(domain::QuantizeReject::NotionalBelowMin { .. })
    ));
}
