//! The property that milestone 1 named as a gate item: **every order the
//! quantizer accepts satisfies every filter it was quantized against**.
//!
//! Exercised through the public API only, over a deterministic sweep of
//! generated prices, quantities and filter sets.
//!
//! # Why a hand-rolled generator rather than `proptest`
//!
//! `domain`'s dependency list is load-bearing - it is `rust_decimal` and
//! `thiserror`, and the milestone gate greps the dependency tree to prove it.
//! A property crate would add a tree of its own to that grep, for shrinking we
//! do not need here: every case below is a pure function of a fixed seed, so a
//! failure reproduces exactly by rerunning, and the failure message prints the
//! offending case in full. If a later milestone wants shrinking badly enough to
//! justify the dependency, this file is what it replaces.

use domain::{
    quantize, ClientOrderId, Decimal, OrderIntent, OrderKind, Price, Qty, QuantizeReject,
    QuantizedOrder, Side, Symbol, SymbolFilterSpec, SymbolFilters, TimeInForce,
};

/// A deterministic linear congruential generator. No `rand`, no float, no clock.
struct Lcg(u64);

impl Lcg {
    fn next_u64(&mut self) -> u64 {
        // Knuth's MMIX constants; any full-period LCG would do.
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // The low bits of an LCG are famously weak, so use the high ones.
        self.0 >> 11
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// A positive decimal with a mantissa up to `max_mantissa` and a scale up to
    /// `max_scale`, which is how Binance's own numbers are shaped.
    fn decimal(&mut self, max_mantissa: u64, max_scale: u32) -> Decimal {
        let mantissa = self.below(max_mantissa) + 1;
        let scale = u32::try_from(self.below(u64::from(max_scale) + 1)).expect("small");
        Decimal::from_i128_with_scale(i128::from(mantissa), scale)
    }
}

fn symbol() -> Symbol {
    Symbol::new("BTCUSDT").expect("valid symbol")
}

fn dec(text: &str) -> Decimal {
    Decimal::from_str_exact(text).expect("literal decimal")
}

fn filters(spec: SymbolFilterSpec) -> SymbolFilters {
    SymbolFilters::new(spec).expect("representative filters must validate")
}

/// Representative filter sets: the real testnet BTCUSDT one, a coarse-tick
/// symbol, a fine-tick one, a symbol whose bounds are not whole numbers of
/// steps, and one with every rule disabled.
fn filter_sets() -> Vec<SymbolFilters> {
    let base = SymbolFilterSpec {
        symbol: symbol(),
        tick_size: Price(dec("0.01000000")),
        min_price: Price(dec("0.01000000")),
        max_price: Price(dec("1000000.00000000")),
        step_size: Qty(dec("0.00001000")),
        min_qty: Qty(dec("0.00001000")),
        max_qty: Qty(dec("9000.00000000")),
        min_notional: dec("5.00000000"),
    };

    vec![
        filters(base.clone()),
        filters(SymbolFilterSpec {
            tick_size: Price(dec("0.5")),
            min_price: Price(dec("1")),
            max_price: Price(dec("500")),
            step_size: Qty(dec("0.1")),
            min_qty: Qty(dec("0.5")),
            max_qty: Qty(dec("100")),
            min_notional: dec("10"),
            ..base.clone()
        }),
        filters(SymbolFilterSpec {
            tick_size: Price(dec("0.00000001")),
            min_price: Price(dec("0.00000100")),
            max_price: Price(dec("1000")),
            step_size: Qty(dec("1")),
            min_qty: Qty(dec("1")),
            max_qty: Qty(dec("90000")),
            min_notional: dec("0.0001"),
            ..base.clone()
        }),
        filters(SymbolFilterSpec {
            tick_size: Price(dec("0.03")),
            min_price: Price(dec("0.09")),
            max_price: Price(dec("9999.99")),
            step_size: Qty(dec("0.07")),
            min_qty: Qty(dec("0.5")),
            max_qty: Qty(dec("70")),
            min_notional: dec("25.5"),
            ..base.clone()
        }),
        // Every rule disabled, which Binance spells as a zero bound.
        filters(SymbolFilterSpec {
            tick_size: Price(Decimal::ZERO),
            min_price: Price(Decimal::ZERO),
            max_price: Price(Decimal::ZERO),
            step_size: Qty(Decimal::ZERO),
            min_qty: Qty(Decimal::ZERO),
            max_qty: Qty(Decimal::ZERO),
            min_notional: Decimal::ZERO,
            ..base
        }),
    ]
}

/// An independent implementation of the alignment, by division rather than by
/// remainder. If the two ever disagree, one of them is wrong and the test says
/// which case found it.
fn floor_to(value: Decimal, size: Decimal) -> Decimal {
    if size.is_zero() {
        value
    } else {
        (value / size).floor() * size
    }
}

fn ceil_to(value: Decimal, size: Decimal) -> Decimal {
    if size.is_zero() {
        value
    } else {
        (value / size).ceil() * size
    }
}

/// Every modeled filter, checked against an accepted order. This is the
/// milestone's gate item spelled out.
fn assert_satisfies_every_filter(order: &QuantizedOrder, filters: &SymbolFilters, case: &str) {
    let price = order.price().0;
    let qty = order.qty().0;

    assert!(price > Decimal::ZERO, "{case}: price must be positive");
    assert!(qty > Decimal::ZERO, "{case}: qty must be positive");

    let tick = filters.tick_size().0;
    if !tick.is_zero() {
        assert_eq!(
            price.checked_rem(tick).expect("remainder"),
            Decimal::ZERO,
            "{case}: price {price} is not a multiple of tick {tick}"
        );
        // Exactness twice over: the value sits on a tick, and it *serialises* at
        // the tick's precision. A value-correct, wrong-scale price is what the
        // exchange rejects.
        assert_eq!(price.scale(), tick.scale(), "{case}: price scale");
    }
    let step = filters.step_size().0;
    if !step.is_zero() {
        assert_eq!(
            qty.checked_rem(step).expect("remainder"),
            Decimal::ZERO,
            "{case}: qty {qty} is not a multiple of step {step}"
        );
        assert_eq!(qty.scale(), step.scale(), "{case}: qty scale");
    }

    if !filters.min_price().0.is_zero() {
        assert!(
            order.price() >= filters.min_price(),
            "{case}: price below minPrice"
        );
    }
    if !filters.max_price().0.is_zero() {
        assert!(
            order.price() <= filters.max_price(),
            "{case}: price above maxPrice"
        );
    }
    if !filters.min_qty().0.is_zero() {
        assert!(order.qty() >= filters.min_qty(), "{case}: qty below minQty");
    }
    if !filters.max_qty().0.is_zero() {
        assert!(order.qty() <= filters.max_qty(), "{case}: qty above maxQty");
    }

    let notional = price.checked_mul(qty).expect("notional");
    assert_eq!(order.notional(), notional, "{case}: notional recomputes");
    assert!(
        notional >= filters.min_notional(),
        "{case}: notional {notional} below minNotional {}",
        filters.min_notional()
    );
}

fn intent(case: u32, side: Side, price: Decimal, qty: Decimal) -> OrderIntent {
    OrderIntent {
        client_order_id: ClientOrderId(format!("case-{case}")),
        symbol: symbol(),
        side,
        kind: OrderKind::Limit {
            price: Price(price),
        },
        qty: Qty(qty),
        tif: TimeInForce::Gtc,
    }
}

#[test]
fn every_accepted_order_satisfies_every_modeled_filter() {
    let sets = filter_sets();
    let mut rng = Lcg(0x5EED_1234_9ABC_DEF0);
    let mut accepted = 0_u32;
    let mut rejected = 0_u32;

    for case in 0..40_000_u32 {
        let filters = &sets[usize::try_from(case).expect("fits") % sets.len()];
        let side = if rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        // Prices and quantities across the bands Binance spot actually spans, so
        // both the accept and the reject paths get a real workout.
        let price = rng.decimal(100_000_000, 8);
        let qty = rng.decimal(10_000_000, 8);
        let intent = intent(case, side, price, qty);
        let label = format!("case {case}: {side:?} {qty} @ {price} against {filters:?}");

        let tick = filters.tick_size().0;
        let step = filters.step_size().0;

        match quantize(&intent, filters) {
            Ok(order) => {
                accepted += 1;
                assert_satisfies_every_filter(&order, filters, &label);

                // Rounding direction, per side. A buy never bids more than it
                // was asked to, a sell never offers less, and neither moves by a
                // whole tick.
                match side {
                    Side::Buy => assert!(order.price().0 <= price, "{label}: a buy rounded up"),
                    Side::Sell => {
                        assert!(order.price().0 >= price, "{label}: a sell rounded down");
                    }
                }
                if !tick.is_zero() {
                    assert!(
                        (order.price().0 - price).abs() < tick,
                        "{label}: the price moved a whole tick or more"
                    );
                }
                // A quantity is never inflated, and never shrunk by a whole step.
                assert!(order.qty().0 <= qty, "{label}: qty was rounded up");
                if !step.is_zero() {
                    assert!(qty - order.qty().0 < step, "{label}: qty lost a whole step");
                }

                // The independent, division-based alignment agrees.
                let expected_price = match side {
                    Side::Buy => floor_to(price, tick),
                    Side::Sell => ceil_to(price, tick),
                };
                assert_eq!(order.price().0, expected_price, "{label}: price alignment");
                assert_eq!(order.qty().0, floor_to(qty, step), "{label}: qty alignment");

                // Pure: the same inputs give the same answer, every time.
                assert_eq!(
                    quantize(&intent, filters).expect("still accepted"),
                    order,
                    "{label}: not deterministic"
                );
            }
            Err(reject) => {
                rejected += 1;
                // A rejection has to be justified by the *aligned* values, which
                // the independent implementation recomputes here.
                let aligned_price = match side {
                    Side::Buy => floor_to(price, tick),
                    Side::Sell => ceil_to(price, tick),
                };
                let aligned_qty = floor_to(qty, step);
                let notional = aligned_price
                    .checked_mul(aligned_qty)
                    .expect("notional in range");

                match reject {
                    QuantizeReject::PriceBelowMin { .. } => assert!(
                        aligned_price <= Decimal::ZERO || aligned_price < filters.min_price().0,
                        "{label}: rejected as below minPrice, but it is not"
                    ),
                    QuantizeReject::PriceAboveMax { .. } => assert!(
                        aligned_price > filters.max_price().0,
                        "{label}: rejected as above maxPrice, but it is not"
                    ),
                    QuantizeReject::QtyBelowMin { .. } => assert!(
                        aligned_qty <= Decimal::ZERO || aligned_qty < filters.min_qty().0,
                        "{label}: rejected as below minQty, but it is not"
                    ),
                    QuantizeReject::QtyAboveMax { .. } => assert!(
                        aligned_qty > filters.max_qty().0,
                        "{label}: rejected as above maxQty, but it is not"
                    ),
                    QuantizeReject::NotionalBelowMin { .. } => assert!(
                        notional < filters.min_notional(),
                        "{label}: rejected as below minNotional, but it is not"
                    ),
                    other => panic!("{label}: unreachable rejection {other:?}"),
                }
            }
        }
    }

    // A property test that only ever took one branch would prove nothing.
    assert!(accepted > 1_000, "only {accepted} orders were accepted");
    assert!(rejected > 1_000, "only {rejected} orders were rejected");
}

#[test]
fn every_rejection_reason_is_reachable_from_generated_input() {
    // The companion to the sweep above: that one asserts each rejection it saw
    // was justified, and this one asserts the sweep really does see them all. A
    // reason no input can produce is either dead code or a hole in the generator.
    let sets = filter_sets();
    let mut rng = Lcg(0xC0FF_EE00_1357_9BDF);
    let mut seen_price_low = false;
    let mut seen_price_high = false;
    let mut seen_qty_low = false;
    let mut seen_qty_high = false;
    let mut seen_notional = false;

    for case in 0..40_000_u32 {
        let filters = &sets[usize::try_from(case).expect("fits") % sets.len()];
        let side = if rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let intent = intent(
            case,
            side,
            rng.decimal(100_000_000, 8),
            rng.decimal(10_000_000, 8),
        );

        match quantize(&intent, filters) {
            Ok(_) => {}
            Err(QuantizeReject::PriceBelowMin { .. }) => seen_price_low = true,
            Err(QuantizeReject::PriceAboveMax { .. }) => seen_price_high = true,
            Err(QuantizeReject::QtyBelowMin { .. }) => seen_qty_low = true,
            Err(QuantizeReject::QtyAboveMax { .. }) => seen_qty_high = true,
            Err(QuantizeReject::NotionalBelowMin { .. }) => seen_notional = true,
            Err(other) => panic!("case {case}: unexpected rejection {other:?}"),
        }
    }

    assert!(seen_price_low, "no price-below-minimum case was generated");
    assert!(seen_price_high, "no price-above-maximum case was generated");
    assert!(seen_qty_low, "no qty-below-minimum case was generated");
    assert!(seen_qty_high, "no qty-above-maximum case was generated");
    assert!(
        seen_notional,
        "no notional-below-minimum case was generated"
    );
}
