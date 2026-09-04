//! The exchange trading rules we model, per symbol.
//!
//! Pure data, validated at construction. This type lives in `domain` rather than
//! in the exchange adapter for the same reason [`crate::quantize`] does: a
//! backtest has to align orders to the same tick and step a live run does, so
//! both must read the same rules through the same type.
//!
//! # Zero means "rule disabled"
//!
//! Confirmed against the current Binance spot filter documentation: within a
//! symbol filter, "any of the above variables can be set to 0, which disables
//! that rule". A `maxQty` of `0` is not a symbol nobody can trade - it is a
//! symbol with no upper quantity bound, and `MARKET_LOT_SIZE` on testnet
//! BTCUSDT really does ship `"stepSize": "0.00000000"`. Treating a zero bound as
//! a literal bound would reject every order on such a symbol; treating a zero
//! `tickSize` as a divisor would panic. Both are handled explicitly here and in
//! [`crate::quantize`], and the *disabled* reading never makes an order less
//! valid: a price still has to be strictly positive, whatever `minPrice` says.

use rust_decimal::Decimal;

use crate::types::{Price, Qty, Symbol};

/// The trading filters we model, per symbol. Decimals throughout.
///
/// Fields are private and there is no field-by-field constructor: the only way
/// to build one is [`SymbolFilters::new`], so a `SymbolFilters` value is a claim
/// that the rules it carries are internally consistent. [`crate::quantize`]
/// relies on that claim - it is what lets alignment be arithmetic rather than a
/// pile of defensive branches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymbolFilters {
    symbol: Symbol,
    tick_size: Price,
    min_price: Price,
    max_price: Price,
    step_size: Qty,
    min_qty: Qty,
    max_qty: Qty,
    min_notional: Decimal,
}

/// The rules as they arrive from the exchange, before validation.
///
/// A plain named-field bag so the eight same-shaped decimals are impossible to
/// transpose at a call site. [`SymbolFilters::new`] turns one of these into the
/// validated type, or refuses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymbolFilterSpec {
    pub symbol: Symbol,
    /// `PRICE_FILTER.tickSize`. `0` disables the tick rule.
    pub tick_size: Price,
    /// `PRICE_FILTER.minPrice`. `0` disables the lower bound.
    pub min_price: Price,
    /// `PRICE_FILTER.maxPrice`. `0` disables the upper bound.
    pub max_price: Price,
    /// `LOT_SIZE.stepSize`. `0` disables the step rule.
    pub step_size: Qty,
    /// `LOT_SIZE.minQty`. `0` disables the lower bound.
    pub min_qty: Qty,
    /// `LOT_SIZE.maxQty`. `0` disables the upper bound.
    pub max_qty: Qty,
    /// `NOTIONAL.minNotional`, as it applies to limit orders. `0` disables it.
    pub min_notional: Decimal,
}

/// A set of filters we refuse to hold, because no order could be checked
/// honestly against them.
///
/// Fail-closed: these are the shapes that would otherwise turn into a panic
/// (dividing by a zero we did not expect) or into silence (a range that rejects
/// everything). Better to refuse to start than to quantize against nonsense.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SymbolFilterError {
    #[error("{symbol}: `{field}` is {value}; a filter bound must not be negative")]
    Negative {
        symbol: Symbol,
        field: &'static str,
        value: Decimal,
    },

    #[error(
        "{symbol}: `{min_field}` ({min}) is above `{max_field}` ({max}); \
         no order could satisfy both"
    )]
    InvertedRange {
        symbol: Symbol,
        min_field: &'static str,
        min: Decimal,
        max_field: &'static str,
        max: Decimal,
    },
}

impl SymbolFilters {
    /// Validate a set of exchange filters.
    ///
    /// # Errors
    ///
    /// [`SymbolFilterError::Negative`] for a negative bound, and
    /// [`SymbolFilterError::InvertedRange`] for a min above its max. A `0`
    /// bound is *not* an error - see the module docs.
    pub fn new(spec: SymbolFilterSpec) -> Result<Self, SymbolFilterError> {
        let SymbolFilterSpec {
            symbol,
            tick_size,
            min_price,
            max_price,
            step_size,
            min_qty,
            max_qty,
            min_notional,
        } = spec;

        for (field, value) in [
            ("tickSize", tick_size.0),
            ("minPrice", min_price.0),
            ("maxPrice", max_price.0),
            ("stepSize", step_size.0),
            ("minQty", min_qty.0),
            ("maxQty", max_qty.0),
            ("minNotional", min_notional),
        ] {
            if value.is_sign_negative() && !value.is_zero() {
                return Err(SymbolFilterError::Negative {
                    symbol,
                    field,
                    value,
                });
            }
        }

        // A zero max is "no upper bound", so it cannot invert a range.
        if !max_price.0.is_zero() && min_price > max_price {
            return Err(SymbolFilterError::InvertedRange {
                symbol,
                min_field: "minPrice",
                min: min_price.0,
                max_field: "maxPrice",
                max: max_price.0,
            });
        }
        if !max_qty.0.is_zero() && min_qty > max_qty {
            return Err(SymbolFilterError::InvertedRange {
                symbol,
                min_field: "minQty",
                min: min_qty.0,
                max_field: "maxQty",
                max: max_qty.0,
            });
        }

        Ok(Self {
            symbol,
            tick_size,
            min_price,
            max_price,
            step_size,
            min_qty,
            max_qty,
            min_notional,
        })
    }

    #[must_use]
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    #[must_use]
    pub fn tick_size(&self) -> Price {
        self.tick_size
    }

    #[must_use]
    pub fn min_price(&self) -> Price {
        self.min_price
    }

    #[must_use]
    pub fn max_price(&self) -> Price {
        self.max_price
    }

    #[must_use]
    pub fn step_size(&self) -> Qty {
        self.step_size
    }

    #[must_use]
    pub fn min_qty(&self) -> Qty {
        self.min_qty
    }

    #[must_use]
    pub fn max_qty(&self) -> Qty {
        self.max_qty
    }

    #[must_use]
    pub fn min_notional(&self) -> Decimal {
        self.min_notional
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    /// BTCUSDT as spot testnet actually reports it (captured 2026-09-04).
    pub(crate) fn btcusdt_spec() -> SymbolFilterSpec {
        SymbolFilterSpec {
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            tick_size: Price(dec!(0.01000000)),
            min_price: Price(dec!(0.01000000)),
            max_price: Price(dec!(1000000.00000000)),
            step_size: Qty(dec!(0.00001000)),
            min_qty: Qty(dec!(0.00001000)),
            max_qty: Qty(dec!(9000.00000000)),
            min_notional: dec!(5.00000000),
        }
    }

    #[test]
    fn real_testnet_filters_are_accepted_and_read_back_exactly() {
        let filters = SymbolFilters::new(btcusdt_spec()).expect("real filters must validate");
        assert_eq!(filters.symbol().as_str(), "BTCUSDT");
        assert_eq!(filters.tick_size(), Price(dec!(0.01)));
        assert_eq!(filters.min_notional(), dec!(5));
        // Scale survives construction: it is what the wire format must match.
        assert_eq!(filters.tick_size().0.scale(), 8);
        assert_eq!(filters.step_size().0.scale(), 8);
    }

    #[test]
    fn zero_bounds_mean_the_rule_is_disabled_not_that_the_symbol_is_untradeable() {
        // MARKET_LOT_SIZE on testnet BTCUSDT really does carry a zero stepSize
        // and minQty. A symbol whose LOT_SIZE looked like that must still build.
        let spec = SymbolFilterSpec {
            step_size: Qty(dec!(0)),
            min_qty: Qty(dec!(0)),
            max_qty: Qty(dec!(0)),
            min_price: Price(dec!(0)),
            max_price: Price(dec!(0)),
            min_notional: dec!(0),
            ..btcusdt_spec()
        };
        assert!(SymbolFilters::new(spec).is_ok());
    }

    #[test]
    fn a_negative_bound_is_refused() {
        for (field, spec) in [
            (
                "tickSize",
                SymbolFilterSpec {
                    tick_size: Price(dec!(-0.01)),
                    ..btcusdt_spec()
                },
            ),
            (
                "minQty",
                SymbolFilterSpec {
                    min_qty: Qty(dec!(-1)),
                    ..btcusdt_spec()
                },
            ),
            (
                "minNotional",
                SymbolFilterSpec {
                    min_notional: dec!(-5),
                    ..btcusdt_spec()
                },
            ),
        ] {
            let err = SymbolFilters::new(spec).expect_err("must refuse a negative bound");
            let SymbolFilterError::Negative { field: got, .. } = err else {
                panic!("expected a Negative error, got {err:?}");
            };
            assert_eq!(got, field);
        }
    }

    #[test]
    fn an_inverted_range_is_refused_but_a_zero_max_is_not() {
        let inverted = SymbolFilterSpec {
            min_price: Price(dec!(10)),
            max_price: Price(dec!(1)),
            ..btcusdt_spec()
        };
        assert!(matches!(
            SymbolFilters::new(inverted),
            Err(SymbolFilterError::InvertedRange { .. })
        ));

        let inverted_qty = SymbolFilterSpec {
            min_qty: Qty(dec!(10)),
            max_qty: Qty(dec!(1)),
            ..btcusdt_spec()
        };
        assert!(matches!(
            SymbolFilters::new(inverted_qty),
            Err(SymbolFilterError::InvertedRange { .. })
        ));

        // A zero max is "no bound", so it is not inverted however large the min.
        let unbounded = SymbolFilterSpec {
            min_qty: Qty(dec!(10)),
            max_qty: Qty(dec!(0)),
            ..btcusdt_spec()
        };
        assert!(SymbolFilters::new(unbounded).is_ok());
    }

    #[test]
    fn the_refusal_names_the_symbol_and_the_offending_field() {
        let err = SymbolFilters::new(SymbolFilterSpec {
            min_price: Price(dec!(-1)),
            ..btcusdt_spec()
        })
        .expect_err("must refuse");
        let msg = err.to_string();
        assert!(msg.contains("BTCUSDT"), "{msg}");
        assert!(msg.contains("minPrice"), "{msg}");
    }
}
