//! Raw [`OrderIntent`] -> exchange-valid [`QuantizedOrder`], or a typed refusal.
//!
//! **Pure**, and here rather than in the exchange adapter on purpose. A backtest
//! that aligns orders to a different tick or step than the live path does
//! produces fills that never could have happened, and the discrepancy would be
//! invisible: both runs "work", and only the money disagrees. Keeping the one
//! implementation in `domain` - no clock, no I/O, no network - is what makes a
//! backtested fill worth believing.
//!
//! # Rounding, and why it only ever goes one way
//!
//! - **Quantity always floors** to `stepSize`. Rounding a size *up* can exceed
//!   the balance we have or the risk we intended; rounding it down cannot.
//! - **A limit price rounds to the less aggressive tick for its side**: a buy
//!   floors, a sell ceils. The order therefore never crosses further than the
//!   caller asked for. This trades a little fill probability for never being
//!   surprised by where we bought, which is the right bias for the first orders
//!   this system sends.
//!
//! There is deliberately no `RoundingPolicy` parameter. No caller needs
//! nearest-or-aggressive rounding yet, and this project draws an abstraction
//! around the second real case, not the first imagined one. If a strategy in a
//! later milestone genuinely needs aggressive quoting, the parameter is added
//! then, with that strategy as its justification.
//!
//! # Rounding past a floor is a rejection, never a shrink
//!
//! If flooring a quantity puts it under `minQty`, or the rounded price times the
//! floored quantity lands under `minNotional`, the answer is a typed rejection.
//! Silently sending a resized order - or one the exchange will bounce - is
//! exactly the quiet wrongness this project refuses. The caller asked for
//! something that cannot be expressed under these filters, and it is entitled to
//! be told so.
//!
//! # Exactness
//!
//! Alignment is `value - (value % size)`, computed in [`Decimal`]. The remainder
//! is exact, which division-then-floor would not be: a quotient that does not
//! terminate inside 28 significant digits would round, and a price that is one
//! ulp off a tick boundary is rejected by the exchange. The aligned value is
//! then rescaled to the filter's own scale, so `to_string()` carries exactly the
//! precision the exchange published - a value-correct, wrong-scale quantity is
//! precisely what gets rejected on the wire.

use rust_decimal::Decimal;

use crate::filters::SymbolFilters;
use crate::types::{ClientOrderId, OrderIntent, OrderKind, Price, Qty, Side, Symbol, TimeInForce};

/// A validated, exchange-ready order.
///
/// Holding one is a claim that it satisfies every filter this build models.
/// There is no public constructor and no public field: [`quantize`] is the only
/// way to obtain one, so an unvalidated `QuantizedOrder` is unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuantizedOrder {
    client_order_id: ClientOrderId,
    symbol: Symbol,
    side: Side,
    /// Aligned to `tickSize`, at the tick's own scale.
    price: Price,
    /// Aligned to `stepSize`, at the step's own scale.
    qty: Qty,
    /// `price * qty`, kept because it is the value that was checked against
    /// `minNotional`. Recomputing it later would be a second chance to disagree.
    notional: Decimal,
    tif: TimeInForce,
}

/// Why an intent cannot become an order under these filters.
///
/// Every variant carries the offending value and the bound it broke, because the
/// first thing an operator asks of a rejected order is "by how much?".
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum QuantizeReject {
    #[error(
        "intent is for `{intent}` but the filters are for `{filters}`; refusing to \
         quantize one symbol's order against another symbol's rules"
    )]
    SymbolMismatch { intent: Symbol, filters: Symbol },

    #[error(
        "price {rounded} (from {requested}, rounded to a {tick_size} tick) is not a \
         valid limit price: it must be strictly positive and at least minPrice {min_price}"
    )]
    PriceBelowMin {
        requested: Price,
        rounded: Price,
        min_price: Price,
        tick_size: Price,
    },

    #[error("price {rounded} (from {requested}) is above maxPrice {max_price}")]
    PriceAboveMax {
        requested: Price,
        rounded: Price,
        max_price: Price,
    },

    #[error(
        "qty {rounded} (from {requested}, floored to a {step_size} step) is not a \
         valid size: it must be strictly positive and at least minQty {min_qty}"
    )]
    QtyBelowMin {
        requested: Qty,
        rounded: Qty,
        min_qty: Qty,
        step_size: Qty,
    },

    #[error(
        "qty {rounded} is above maxQty {max_qty}; refusing to shrink the order to \
         fit, because a size nobody asked for is not a safer size"
    )]
    QtyAboveMax { rounded: Qty, max_qty: Qty },

    #[error("notional {notional} ({price} x {qty}) is below minNotional {min_notional}")]
    NotionalBelowMin {
        price: Price,
        qty: Qty,
        notional: Decimal,
        min_notional: Decimal,
    },

    #[error(
        "{price} x {qty} is not exactly representable as a decimal, so the notional \
         cannot be checked; refusing rather than guessing"
    )]
    NotionalUnrepresentable { price: Price, qty: Qty },

    #[error(
        "market-order quantization is deferred to milestone 6: it needs \
         MARKET_LOT_SIZE and an average-price reference for the notional check, \
         and this build has neither"
    )]
    MarketNotSupported,
}

impl QuantizedOrder {
    #[must_use]
    pub fn client_order_id(&self) -> &ClientOrderId {
        &self.client_order_id
    }

    #[must_use]
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    #[must_use]
    pub fn side(&self) -> Side {
        self.side
    }

    #[must_use]
    pub fn price(&self) -> Price {
        self.price
    }

    #[must_use]
    pub fn qty(&self) -> Qty {
        self.qty
    }

    /// `price * qty`, exactly as it was checked against `minNotional`.
    #[must_use]
    pub fn notional(&self) -> Decimal {
        self.notional
    }

    #[must_use]
    pub fn time_in_force(&self) -> TimeInForce {
        self.tif
    }
}

/// Align a raw intent to a symbol's filters.
///
/// Pure: the same intent and the same filters give the same answer on every
/// machine, live or in replay.
///
/// # Errors
///
/// [`QuantizeReject`], one variant per modeled rule the intent cannot satisfy.
/// A rejection is never a partially-applied order: nothing is resized, nothing
/// is clamped.
pub fn quantize(
    intent: &OrderIntent,
    filters: &SymbolFilters,
) -> Result<QuantizedOrder, QuantizeReject> {
    if intent.symbol != *filters.symbol() {
        return Err(QuantizeReject::SymbolMismatch {
            intent: intent.symbol.clone(),
            filters: filters.symbol().clone(),
        });
    }

    // Market orders are refused loudly rather than mishandled quietly. See
    // `QuantizeReject::MarketNotSupported`.
    let OrderKind::Limit { price: requested } = intent.kind else {
        return Err(QuantizeReject::MarketNotSupported);
    };

    let price = align_price(requested, intent.side, filters)?;
    let qty = align_qty(intent.qty, filters)?;

    let notional = price
        .0
        .checked_mul(qty.0)
        .ok_or(QuantizeReject::NotionalUnrepresentable { price, qty })?;
    let min_notional = filters.min_notional();
    if !min_notional.is_zero() && notional < min_notional {
        return Err(QuantizeReject::NotionalBelowMin {
            price,
            qty,
            notional,
            min_notional,
        });
    }

    Ok(QuantizedOrder {
        client_order_id: intent.client_order_id.clone(),
        symbol: intent.symbol.clone(),
        side: intent.side,
        price,
        qty,
        notional,
        tif: intent.tif,
    })
}

/// Round a limit price to the less aggressive tick for its side, then bound it.
fn align_price(
    requested: Price,
    side: Side,
    filters: &SymbolFilters,
) -> Result<Price, QuantizeReject> {
    let tick_size = filters.tick_size();
    let min_price = filters.min_price();
    let below_min = |rounded| QuantizeReject::PriceBelowMin {
        requested,
        rounded,
        min_price,
        tick_size,
    };

    // Alignment is defined on non-negative values only; a non-positive price is
    // not a price, so it never reaches the arithmetic.
    if requested.0.is_zero() || requested.0.is_sign_negative() {
        return Err(below_min(requested));
    }

    let rounded = Price(match side {
        // A buy that rounds down bids less; a sell that rounds up offers more.
        // Either way the order crosses no further than the caller asked.
        Side::Buy => align_down(requested.0, tick_size.0).ok_or_else(|| below_min(requested))?,
        Side::Sell => align_up(requested.0, tick_size.0).ok_or_else(|| below_min(requested))?,
    });

    if rounded.0.is_zero() || rounded.0.is_sign_negative() || rounded < min_price {
        return Err(below_min(rounded));
    }
    let max_price = filters.max_price();
    if !max_price.0.is_zero() && rounded > max_price {
        return Err(QuantizeReject::PriceAboveMax {
            requested,
            rounded,
            max_price,
        });
    }

    Ok(rounded)
}

/// Floor a quantity to the step, then bound it. Never rounds a size up.
fn align_qty(requested: Qty, filters: &SymbolFilters) -> Result<Qty, QuantizeReject> {
    let step_size = filters.step_size();
    let min_qty = filters.min_qty();
    let below_min = |rounded| QuantizeReject::QtyBelowMin {
        requested,
        rounded,
        min_qty,
        step_size,
    };

    if requested.0.is_zero() || requested.0.is_sign_negative() {
        return Err(below_min(requested));
    }

    let rounded = Qty(align_down(requested.0, step_size.0).ok_or_else(|| below_min(requested))?);

    if rounded.0.is_zero() || rounded.0.is_sign_negative() || rounded < min_qty {
        return Err(below_min(rounded));
    }
    let max_qty = filters.max_qty();
    if !max_qty.0.is_zero() && rounded > max_qty {
        // Deliberately not clamped to max_qty: see the variant's message.
        return Err(QuantizeReject::QtyAboveMax { rounded, max_qty });
    }

    Ok(rounded)
}

/// The largest multiple of `size` that is `<= value`, at `size`'s scale.
///
/// `value` must be non-negative, which every caller checks first. A zero `size`
/// means the rule is disabled and the value passes through unchanged. `None`
/// means the arithmetic could not be done exactly, which callers turn into a
/// refusal rather than an approximation.
fn align_down(value: Decimal, size: Decimal) -> Option<Decimal> {
    if size.is_zero() {
        return Some(value);
    }
    let aligned = value.checked_sub(value.checked_rem(size)?)?;
    rescaled(aligned, size.scale())
}

/// The smallest multiple of `size` that is `>= value`, at `size`'s scale.
///
/// Same contract as [`align_down`].
fn align_up(value: Decimal, size: Decimal) -> Option<Decimal> {
    if size.is_zero() {
        return Some(value);
    }
    let remainder = value.checked_rem(size)?;
    let aligned = if remainder.is_zero() {
        value
    } else {
        value.checked_sub(remainder)?.checked_add(size)?
    };
    rescaled(aligned, size.scale())
}

/// `value` restated at `scale`, or `None` if that would change it.
///
/// The exchange publishes `tickSize` as `"0.01000000"` and expects prices at no
/// more precision than that, so the scale is part of the answer rather than a
/// display detail. `Decimal::rescale` saturates silently when a value will not
/// fit, so the result is compared back: equality on `Decimal` ignores scale, and
/// a mismatch means digits were lost.
fn rescaled(value: Decimal, scale: u32) -> Option<Decimal> {
    let mut restated = value;
    restated.rescale(scale);
    (restated == value).then_some(restated)
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;
    use crate::filters::SymbolFilterSpec;

    /// BTCUSDT as spot testnet reports it, scales and all (captured 2026-09-04).
    fn btcusdt() -> SymbolFilters {
        SymbolFilters::new(SymbolFilterSpec {
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            tick_size: Price(dec!(0.01000000)),
            min_price: Price(dec!(0.01000000)),
            max_price: Price(dec!(1000000.00000000)),
            step_size: Qty(dec!(0.00001000)),
            min_qty: Qty(dec!(0.00001000)),
            max_qty: Qty(dec!(9000.00000000)),
            min_notional: dec!(5.00000000),
        })
        .expect("real filters must validate")
    }

    fn limit(side: Side, price: Decimal, qty: Decimal) -> OrderIntent {
        OrderIntent {
            client_order_id: ClientOrderId("test-1".to_owned()),
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            side,
            kind: OrderKind::Limit {
                price: Price(price),
            },
            qty: Qty(qty),
            tif: TimeInForce::Gtc,
        }
    }

    fn accept(side: Side, price: Decimal, qty: Decimal) -> QuantizedOrder {
        quantize(&limit(side, price, qty), &btcusdt()).expect("should quantize")
    }

    fn reject(side: Side, price: Decimal, qty: Decimal) -> QuantizeReject {
        quantize(&limit(side, price, qty), &btcusdt()).expect_err("should reject")
    }

    #[test]
    fn a_buy_floors_the_price_and_a_sell_ceils_it() {
        // Same raw price, opposite sides, different ticks - and each moves *away*
        // from crossing. Red if someone "simplifies" this to nearest-tick
        // rounding, which would make a buy bid a cent more than it asked to.
        let raw = dec!(65000.123);
        assert_eq!(
            accept(Side::Buy, raw, dec!(0.001)).price(),
            Price(dec!(65000.12))
        );
        assert_eq!(
            accept(Side::Sell, raw, dec!(0.001)).price(),
            Price(dec!(65000.13))
        );
    }

    #[test]
    fn an_already_aligned_price_is_left_alone_on_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            assert_eq!(
                accept(side, dec!(65000.12), dec!(0.001)).price(),
                Price(dec!(65000.12)),
                "{side:?}"
            );
        }
    }

    #[test]
    fn the_result_is_exact_at_value_and_at_scale() {
        // Twice-asserted on purpose. `Decimal` equality ignores trailing zeros,
        // so a value-equal result can still be the wrong string on the wire, and
        // the wire is where the exchange rejects it. The filters here carry
        // Binance's own scale of 8.
        let order = accept(Side::Buy, dec!(65000.123456), dec!(0.0012345678));

        assert_eq!(order.price(), Price(dec!(65000.12)));
        assert_eq!(order.price().0.to_string(), "65000.12000000");
        assert_eq!(order.price().0.scale(), 8);

        // The step is 0.00001, so the request's extra digits are floored away.
        assert_eq!(order.qty(), Qty(dec!(0.00123)));
        assert_eq!(order.qty().0.to_string(), "0.00123000");
        assert_eq!(order.qty().0.scale(), 8);

        // The alignment itself is exact: no remainder against tick or step.
        assert!(order
            .price()
            .0
            .checked_rem(dec!(0.01))
            .expect("rem")
            .is_zero());
        assert!(order
            .qty()
            .0
            .checked_rem(dec!(0.00001))
            .expect("rem")
            .is_zero());
    }

    #[test]
    fn the_scale_follows_the_filter_not_the_request() {
        // A symbol whose tick is published as `0.01` rather than `0.01000000`
        // must produce `65000.12`, not `65000.12000000`.
        let filters = SymbolFilters::new(SymbolFilterSpec {
            tick_size: Price(dec!(0.01)),
            step_size: Qty(dec!(0.001)),
            min_qty: Qty(dec!(0.001)),
            ..SymbolFilterSpec {
                symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
                tick_size: Price(dec!(0.01)),
                min_price: Price(dec!(0.01)),
                max_price: Price(dec!(1000000)),
                step_size: Qty(dec!(0.001)),
                min_qty: Qty(dec!(0.001)),
                max_qty: Qty(dec!(9000)),
                min_notional: dec!(5),
            }
        })
        .expect("filters");
        let order = quantize(&limit(Side::Buy, dec!(65000.1), dec!(0.0019)), &filters)
            .expect("should quantize");
        assert_eq!(order.price().0.to_string(), "65000.10");
        assert_eq!(order.qty().0.to_string(), "0.001");
    }

    #[test]
    fn quantity_always_floors_and_never_inflates_the_size() {
        // 0.0000199 is nearly two steps; nearest-rounding would send two.
        assert_eq!(
            accept(Side::Buy, dec!(600000), dec!(0.0000199)).qty(),
            Qty(dec!(0.00001))
        );
    }

    #[test]
    fn exactly_min_qty_is_accepted_and_a_hair_under_it_is_rejected() {
        // The boundary itself, from both sides. `min_qty` is one step here, so
        // "one step below" is zero - the case that must reject rather than send
        // an empty order.
        let order = accept(Side::Buy, dec!(600000), dec!(0.00001));
        assert_eq!(order.qty(), Qty(dec!(0.00001)));

        let err = reject(Side::Buy, dec!(600000), dec!(0.000009999));
        let QuantizeReject::QtyBelowMin {
            rounded, min_qty, ..
        } = err
        else {
            panic!("expected QtyBelowMin, got {err:?}");
        };
        assert_eq!(rounded, Qty(dec!(0)), "it floored to nothing");
        assert_eq!(min_qty, Qty(dec!(0.00001)));
    }

    #[test]
    fn a_qty_that_floors_to_just_under_min_qty_is_rejected_not_rounded_up() {
        // A symbol whose minQty is not itself one step: 0.5 minimum, 0.1 step.
        // 0.49 floors to 0.4, which is under the minimum. Rounding *up* to 0.5
        // would be the silent resize this project refuses.
        let filters = SymbolFilters::new(SymbolFilterSpec {
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            tick_size: Price(dec!(0.01)),
            min_price: Price(dec!(0.01)),
            max_price: Price(dec!(1000000)),
            step_size: Qty(dec!(0.1)),
            min_qty: Qty(dec!(0.5)),
            max_qty: Qty(dec!(9000)),
            min_notional: dec!(5),
        })
        .expect("filters");

        let err =
            quantize(&limit(Side::Buy, dec!(100), dec!(0.49)), &filters).expect_err("must reject");
        assert!(
            matches!(
                err,
                QuantizeReject::QtyBelowMin {
                    rounded: Qty(r),
                    ..
                } if r == dec!(0.4)
            ),
            "{err:?}"
        );
        // And exactly the minimum still goes through.
        assert!(quantize(&limit(Side::Buy, dec!(100), dec!(0.5)), &filters).is_ok());
    }

    #[test]
    fn a_qty_above_max_qty_is_rejected_rather_than_clamped() {
        let err = reject(Side::Buy, dec!(0.01), dec!(9000.00001));
        let QuantizeReject::QtyAboveMax { rounded, max_qty } = err else {
            panic!("expected QtyAboveMax, got {err:?}");
        };
        assert_eq!(rounded, Qty(dec!(9000.00001)));
        assert_eq!(max_qty, Qty(dec!(9000)));
        // Exactly max is fine.
        assert_eq!(
            accept(Side::Buy, dec!(0.01), dec!(9000)).qty(),
            Qty(dec!(9000))
        );
    }

    #[test]
    fn a_price_one_tick_outside_the_range_is_rejected_on_the_correct_side() {
        // Below the floor: a buy floors 0.005 to 0.00, which is no price at all.
        let err = reject(Side::Buy, dec!(0.005), dec!(1000));
        assert!(
            matches!(err, QuantizeReject::PriceBelowMin { .. }),
            "{err:?}"
        );
        // The same raw price on a sell ceils *up* to exactly minPrice, and that
        // is a legitimate order. Rounding direction is per side, not per bound.
        assert_eq!(
            accept(Side::Sell, dec!(0.005), dec!(1000)).price(),
            Price(dec!(0.01))
        );

        // Above the ceiling, one tick out.
        let err = reject(Side::Buy, dec!(1000000.01), dec!(0.001));
        let QuantizeReject::PriceAboveMax {
            rounded, max_price, ..
        } = err
        else {
            panic!("expected PriceAboveMax, got {err:?}");
        };
        assert_eq!(rounded, Price(dec!(1000000.01)));
        assert_eq!(max_price, Price(dec!(1000000)));
        // Exactly maxPrice is inside the range.
        assert_eq!(
            accept(Side::Buy, dec!(1000000), dec!(0.001)).price(),
            Price(dec!(1000000))
        );
    }

    #[test]
    fn a_non_positive_price_or_qty_is_rejected_before_any_arithmetic() {
        for price in [dec!(0), dec!(-1)] {
            assert!(
                matches!(
                    reject(Side::Buy, price, dec!(1)),
                    QuantizeReject::PriceBelowMin { .. }
                ),
                "price {price}"
            );
        }
        for qty in [dec!(0), dec!(-1)] {
            assert!(
                matches!(
                    reject(Side::Buy, dec!(65000), qty),
                    QuantizeReject::QtyBelowMin { .. }
                ),
                "qty {qty}"
            );
        }
    }

    #[test]
    fn a_notional_one_cent_under_the_minimum_is_rejected() {
        // 100.00 x 0.04999 = 4.999, a cent under the 5.00 minimum. Both the
        // price and the quantity are perfectly aligned - this is the rejection
        // that only the notional check catches.
        let err = reject(Side::Buy, dec!(100), dec!(0.04999));
        let QuantizeReject::NotionalBelowMin {
            notional,
            min_notional,
            ..
        } = err
        else {
            panic!("expected NotionalBelowMin, got {err:?}");
        };
        assert_eq!(notional, dec!(4.999));
        assert_eq!(min_notional, dec!(5));

        // Exactly the minimum is accepted, and the order carries the notional
        // that was checked rather than one recomputed later.
        let order = accept(Side::Buy, dec!(100), dec!(0.05));
        assert_eq!(order.notional(), dec!(5));
    }

    #[test]
    fn rounding_is_what_pushes_this_one_under_the_minimum_and_it_still_rejects() {
        // The composed case, and the reason the notional is checked *after*
        // rounding rather than before: as asked for, 100.009 x 0.049999 is
        // 5.00035, comfortably over the minimum. Aligned - the price floored to
        // the tick, the qty floored to the step - it is 4.999, and under. The
        // order that would actually be sent is the one that has to pass.
        let raw = dec!(100.009).checked_mul(dec!(0.049999)).expect("exact");
        assert!(
            raw > dec!(5),
            "the request itself clears the minimum: {raw}"
        );

        let err = reject(Side::Buy, dec!(100.009), dec!(0.049999));
        let QuantizeReject::NotionalBelowMin { notional, .. } = err else {
            panic!("expected NotionalBelowMin, got {err:?}");
        };
        assert_eq!(notional, dec!(4.999));
    }

    #[test]
    fn a_market_order_is_refused_loudly() {
        let intent = OrderIntent {
            kind: OrderKind::Market,
            ..limit(Side::Buy, dec!(65000), dec!(1))
        };
        assert_eq!(
            quantize(&intent, &btcusdt()),
            Err(QuantizeReject::MarketNotSupported)
        );
        assert!(QuantizeReject::MarketNotSupported
            .to_string()
            .contains("milestone 6"));
    }

    #[test]
    fn quantizing_one_symbols_intent_against_anothers_filters_is_refused() {
        let intent = OrderIntent {
            symbol: Symbol::new("ETHUSDT").expect("valid symbol"),
            ..limit(Side::Buy, dec!(65000), dec!(1))
        };
        let err = quantize(&intent, &btcusdt()).expect_err("must refuse");
        assert!(
            matches!(err, QuantizeReject::SymbolMismatch { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn a_disabled_rule_is_a_rule_that_does_not_apply() {
        // Zero bounds mean "no rule" per Binance's own filter documentation. The
        // price still has to be positive; it just is not aligned or bounded.
        let filters = SymbolFilters::new(SymbolFilterSpec {
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            tick_size: Price(dec!(0)),
            min_price: Price(dec!(0)),
            max_price: Price(dec!(0)),
            step_size: Qty(dec!(0)),
            min_qty: Qty(dec!(0)),
            max_qty: Qty(dec!(0)),
            min_notional: dec!(0),
        })
        .expect("filters");

        let order = quantize(&limit(Side::Buy, dec!(0.000000123), dec!(0.7)), &filters)
            .expect("no rule to break");
        assert_eq!(order.price(), Price(dec!(0.000000123)));
        assert_eq!(order.qty(), Qty(dec!(0.7)));

        // Still not a licence to send a zero-priced order.
        assert!(quantize(&limit(Side::Buy, dec!(0), dec!(1)), &filters).is_err());
    }

    #[test]
    fn the_order_carries_the_intents_own_identity_unchanged() {
        let intent = OrderIntent {
            client_order_id: ClientOrderId("idempotency-key-42".to_owned()),
            tif: TimeInForce::Ioc,
            ..limit(Side::Sell, dec!(65000.123), dec!(0.001))
        };
        let order = quantize(&intent, &btcusdt()).expect("should quantize");

        assert_eq!(
            order.client_order_id(),
            &ClientOrderId("idempotency-key-42".to_owned())
        );
        assert_eq!(order.symbol().as_str(), "BTCUSDT");
        assert_eq!(order.side(), Side::Sell);
        assert_eq!(order.time_in_force(), TimeInForce::Ioc);
    }

    #[test]
    fn alignment_helpers_are_exact_on_awkward_sizes() {
        // Division-then-floor would round these; remainder arithmetic does not.
        assert_eq!(align_down(dec!(0.1), dec!(0.03)), Some(dec!(0.09)));
        assert_eq!(align_up(dec!(0.1), dec!(0.03)), Some(dec!(0.12)));
        assert_eq!(align_down(dec!(0.09), dec!(0.03)), Some(dec!(0.09)));
        assert_eq!(align_up(dec!(0.09), dec!(0.03)), Some(dec!(0.09)));
        // A zero size is a disabled rule, not a division by zero.
        assert_eq!(align_down(dec!(1.23), dec!(0)), Some(dec!(1.23)));
        assert_eq!(align_up(dec!(1.23), dec!(0)), Some(dec!(1.23)));
    }
}
