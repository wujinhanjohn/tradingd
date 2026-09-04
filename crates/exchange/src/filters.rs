//! `exchangeInfo` JSON -> [`domain::SymbolFilters`]. **Pure**, by construction.
//!
//! The same discipline as [`crate::normalize`], for the same reason: this is the
//! parse that decides what an order is allowed to look like, so it reads no
//! clock, touches no network, and is a function of its arguments alone.
//!
//! # Decimals, and the banned float path
//!
//! Every numeric filter field arrives as a JSON **string** - confirmed against
//! the live testnet response on 2026-09-04, and against the current filter
//! documentation. A field arriving as a bare JSON *number* is refused rather
//! than read, because `serde_json` stores a non-integral JSON number as an `f64`
//! and reading it back is exactly the float path this project bans.
//! [`Decimal::from_str_exact`] refuses to round rather than silently truncating.
//!
//! # What we model, and what we merely notice
//!
//! Modeled, because the quantizer enforces them: `PRICE_FILTER`, `LOT_SIZE`,
//! and `NOTIONAL` (with the legacy `MIN_NOTIONAL` accepted as a fallback).
//! `MARKET_LOT_SIZE` is parsed and carried but unused - market-order
//! quantization is milestone 6, and having the values in hand means M6 does not
//! have to re-derive this parse under order-path pressure.
//!
//! Everything else - `PERCENT_PRICE_BY_SIDE`, `MAX_NUM_ORDERS`, `ICEBERG_PARTS`,
//! `TRAILING_DELTA` and whatever Binance adds next - is **surfaced, not
//! dropped**: each symbol's unmodeled filter types come back in
//! [`SymbolInfo::unmodeled`] so the caller can log them. They mean the exchange
//! may reject an order the quantizer thinks is fine, and a rejection nobody
//! predicted is much worse at milestone 6 than a warning line is here.
//!
//! Unknown *fields* inside a filter we do model are ignored additively: Binance
//! adding a field must not break a running bot. Unknown *filter types* are the
//! opposite of that - they are new rules, and new rules get said out loud.

use std::collections::BTreeSet;

use domain::{Decimal, Price, Qty, Symbol, SymbolFilterError, SymbolFilterSpec, SymbolFilters};
use serde_json::Value;

/// The filter types this build enforces, plus the one it merely carries.
const PRICE_FILTER: &str = "PRICE_FILTER";
const LOT_SIZE: &str = "LOT_SIZE";
const NOTIONAL: &str = "NOTIONAL";
/// Superseded by `NOTIONAL`, still accepted as a fallback.
const MIN_NOTIONAL: &str = "MIN_NOTIONAL";
/// Parsed, carried, not yet enforced. Milestone 6.
const MARKET_LOT_SIZE: &str = "MARKET_LOT_SIZE";

/// The exchange's trading status for a symbol that is open for business.
pub const STATUS_TRADING: &str = "TRADING";

/// One symbol's entry in an `exchangeInfo` response, parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymbolInfo {
    /// The rules the quantizer enforces.
    pub filters: SymbolFilters,
    /// `"TRADING"`, `"HALT"`, `"BREAK"`. Carried rather than judged: a halted
    /// symbol is worth a warning at startup, and it is milestone 6's business
    /// whether an order may be sent against one.
    pub status: String,
    /// `MARKET_LOT_SIZE`, if the symbol has one. Unused until milestone 6.
    pub market_lot_size: Option<LotSize>,
    /// Filter types present on this symbol that this build does **not** enforce,
    /// sorted and deduplicated. Never silently empty: if it is empty, the symbol
    /// really did carry nothing beyond what we model.
    pub unmodeled: Vec<String>,
}

/// A `LOT_SIZE`-shaped filter. Only used for `MARKET_LOT_SIZE`, which is carried
/// but not enforced yet; the enforced `LOT_SIZE` lives inside
/// [`domain::SymbolFilters`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LotSize {
    pub min_qty: Qty,
    pub max_qty: Qty,
    pub step_size: Qty,
}

/// Every way an `exchangeInfo` payload can fail to become filters.
///
/// All of them mean "refuse to trade this symbol", never "guess a default". A
/// filter we cannot read is a rule we cannot honour, and an order that breaks a
/// rule we did not know about is rejected by the exchange at best.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum FilterParseError {
    #[error("exchangeInfo payload is {found}, expected an object with a `symbols` array")]
    NotAnObject { found: &'static str },

    #[error("exchangeInfo has no `symbols` array")]
    MissingSymbols,

    #[error("exchangeInfo `symbols[{index}]` is {found}, expected an object")]
    SymbolNotAnObject { index: usize, found: &'static str },

    #[error("exchangeInfo `symbols[{index}]`: missing field `{field}`")]
    MissingField { index: usize, field: &'static str },

    #[error("exchangeInfo `symbols[{index}]`: field `{field}` is {found}, expected {expected}")]
    WrongType {
        index: usize,
        field: &'static str,
        expected: &'static str,
        found: &'static str,
    },

    #[error("exchangeInfo `symbols[{index}]` names invalid symbol `{symbol}`")]
    InvalidSymbol { index: usize, symbol: String },

    #[error("exchangeInfo lists `{symbol}` more than once; refusing to pick one")]
    DuplicateSymbol { symbol: String },

    #[error("`{symbol}`: `{filter_type}` appears more than once; refusing to pick one")]
    DuplicateFilter { symbol: String, filter_type: String },

    #[error(
        "`{symbol}`: filter entry {index} has no `filterType`; refusing to parse a rule \
         we cannot name"
    )]
    UnnamedFilter { symbol: String, index: usize },

    #[error(
        "`{symbol}`: no `{filter_type}` filter. Without it there is no rule to align \
         orders to, and an unaligned order is one the exchange rejects"
    )]
    MissingFilter {
        symbol: String,
        filter_type: &'static str,
    },

    #[error("`{symbol}`: `{filter_type}` is missing field `{field}`")]
    MissingFilterField {
        symbol: String,
        filter_type: &'static str,
        field: &'static str,
    },

    #[error(
        "`{symbol}`: `{filter_type}.{field}` is {found}, expected a decimal string. \
         Binance sends every numeric filter field as a string; a JSON number could \
         only be read through a float, which this project bans"
    )]
    NotAString {
        symbol: String,
        filter_type: &'static str,
        field: &'static str,
        found: &'static str,
    },

    #[error(
        "`{symbol}`: `{filter_type}.{field}` = `{value}` is not an exact decimal. \
         Values are parsed with no rounding; a bound we cannot represent exactly is \
         refused rather than approximated"
    )]
    NotADecimal {
        symbol: String,
        filter_type: &'static str,
        field: &'static str,
        value: String,
    },

    #[error("`{symbol}`: {source}")]
    Unusable {
        symbol: String,
        #[source]
        source: SymbolFilterError,
    },

    #[error(
        "exchangeInfo does not list `{symbol}`, which this bot is configured to trade. \
         Refusing to start: a symbol whose rules we cannot read is one we cannot \
         safely place an order on"
    )]
    SymbolNotListed { symbol: String },
}

/// Parse an `exchangeInfo` payload, keeping the symbols we asked for.
///
/// Symbols in the response that we did not ask for are ignored - a full-market
/// response is a valid answer to a narrow question. A symbol we *did* ask for
/// and did not get back is [`FilterParseError::SymbolNotListed`]: fail-closed,
/// never skipped.
///
/// The result is in `wanted` order, so logs and tests read predictably.
///
/// # Errors
///
/// [`FilterParseError`] for any payload we cannot read exactly.
pub fn parse_exchange_info(
    payload: &Value,
    wanted: &[Symbol],
) -> Result<Vec<SymbolInfo>, FilterParseError> {
    if !payload.is_object() {
        return Err(FilterParseError::NotAnObject {
            found: type_name(payload),
        });
    }
    let symbols = payload
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or(FilterParseError::MissingSymbols)?;

    let mut found: Vec<(Symbol, SymbolInfo)> = Vec::new();
    for (index, entry) in symbols.iter().enumerate() {
        let Some(object) = entry.as_object() else {
            return Err(FilterParseError::SymbolNotAnObject {
                index,
                found: type_name(entry),
            });
        };

        let raw = object
            .get("symbol")
            .ok_or(FilterParseError::MissingField {
                index,
                field: "symbol",
            })?
            .as_str()
            .ok_or_else(|| FilterParseError::WrongType {
                index,
                field: "symbol",
                expected: "a string",
                found: type_name(&object["symbol"]),
            })?;
        let symbol = Symbol::new(raw).map_err(|_| FilterParseError::InvalidSymbol {
            index,
            symbol: raw.to_owned(),
        })?;

        // Parse only what we asked for. A full-market payload is ~thousands of
        // symbols, and parsing rules for instruments we will never trade is work
        // that can only produce spurious refusals.
        if !wanted.contains(&symbol) {
            continue;
        }
        if found.iter().any(|(s, _)| *s == symbol) {
            return Err(FilterParseError::DuplicateSymbol {
                symbol: raw.to_owned(),
            });
        }

        let info = parse_symbol(index, &symbol, entry)?;
        found.push((symbol, info));
    }

    wanted
        .iter()
        .map(|want| {
            found
                .iter()
                .find(|(symbol, _)| symbol == want)
                .map(|(_, info)| info.clone())
                .ok_or_else(|| FilterParseError::SymbolNotListed {
                    symbol: want.to_string(),
                })
        })
        .collect()
}

fn parse_symbol(
    index: usize,
    symbol: &Symbol,
    entry: &Value,
) -> Result<SymbolInfo, FilterParseError> {
    let status = entry
        .get("status")
        .ok_or(FilterParseError::MissingField {
            index,
            field: "status",
        })?
        .as_str()
        .ok_or_else(|| FilterParseError::WrongType {
            index,
            field: "status",
            expected: "a string",
            found: type_name(&entry["status"]),
        })?
        .to_owned();

    let filters =
        entry
            .get("filters")
            .and_then(Value::as_array)
            .ok_or(FilterParseError::MissingField {
                index,
                field: "filters",
            })?;

    let name = symbol.to_string();
    let mut price_filter = None;
    let mut lot_size = None;
    let mut notional = None;
    let mut legacy_notional = None;
    let mut market_lot_size = None;
    let mut unmodeled = BTreeSet::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();

    for (position, filter) in filters.iter().enumerate() {
        let filter_type = filter
            .get("filterType")
            .and_then(Value::as_str)
            .ok_or_else(|| FilterParseError::UnnamedFilter {
                symbol: name.clone(),
                index: position,
            })?;
        if !seen.insert(filter_type.to_owned()) {
            return Err(FilterParseError::DuplicateFilter {
                symbol: name.clone(),
                filter_type: filter_type.to_owned(),
            });
        }

        match filter_type {
            PRICE_FILTER => {
                price_filter = Some((
                    Price(decimal(&name, PRICE_FILTER, filter, "tickSize")?),
                    Price(decimal(&name, PRICE_FILTER, filter, "minPrice")?),
                    Price(decimal(&name, PRICE_FILTER, filter, "maxPrice")?),
                ));
            }
            LOT_SIZE => lot_size = Some(lot(&name, LOT_SIZE, filter)?),
            MARKET_LOT_SIZE => market_lot_size = Some(lot(&name, MARKET_LOT_SIZE, filter)?),
            NOTIONAL => notional = Some(decimal(&name, NOTIONAL, filter, "minNotional")?),
            MIN_NOTIONAL => {
                legacy_notional = Some(decimal(&name, MIN_NOTIONAL, filter, "minNotional")?);
            }
            other => {
                unmodeled.insert(other.to_owned());
            }
        }
    }

    let (tick_size, min_price, max_price) =
        price_filter.ok_or_else(|| FilterParseError::MissingFilter {
            symbol: name.clone(),
            filter_type: PRICE_FILTER,
        })?;
    let lot = lot_size.ok_or_else(|| FilterParseError::MissingFilter {
        symbol: name.clone(),
        filter_type: LOT_SIZE,
    })?;
    // `NOTIONAL` supersedes `MIN_NOTIONAL`; if a symbol somehow carries both, the
    // current one wins rather than the parse order deciding.
    let min_notional =
        notional
            .or(legacy_notional)
            .ok_or_else(|| FilterParseError::MissingFilter {
                symbol: name.clone(),
                filter_type: NOTIONAL,
            })?;

    let filters = SymbolFilters::new(SymbolFilterSpec {
        symbol: symbol.clone(),
        tick_size,
        min_price,
        max_price,
        step_size: lot.step_size,
        min_qty: lot.min_qty,
        max_qty: lot.max_qty,
        min_notional,
    })
    .map_err(|source| FilterParseError::Unusable {
        symbol: name.clone(),
        source,
    })?;

    Ok(SymbolInfo {
        filters,
        status,
        market_lot_size,
        unmodeled: unmodeled.into_iter().collect(),
    })
}

fn lot(
    symbol: &str,
    filter_type: &'static str,
    filter: &Value,
) -> Result<LotSize, FilterParseError> {
    Ok(LotSize {
        min_qty: Qty(decimal(symbol, filter_type, filter, "minQty")?),
        max_qty: Qty(decimal(symbol, filter_type, filter, "maxQty")?),
        step_size: Qty(decimal(symbol, filter_type, filter, "stepSize")?),
    })
}

/// Read one numeric filter field, which must be a JSON string.
///
/// The string requirement is the point, not a formality: see the module docs.
fn decimal(
    symbol: &str,
    filter_type: &'static str,
    filter: &Value,
    field: &'static str,
) -> Result<Decimal, FilterParseError> {
    let raw = filter.get(field).filter(|v| !v.is_null()).ok_or_else(|| {
        FilterParseError::MissingFilterField {
            symbol: symbol.to_owned(),
            filter_type,
            field,
        }
    })?;
    let text = raw.as_str().ok_or_else(|| FilterParseError::NotAString {
        symbol: symbol.to_owned(),
        filter_type,
        field,
        found: type_name(raw),
    })?;

    Decimal::from_str_exact(text).map_err(|_| FilterParseError::NotADecimal {
        symbol: symbol.to_owned(),
        filter_type,
        field,
        value: text.to_owned(),
    })
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
