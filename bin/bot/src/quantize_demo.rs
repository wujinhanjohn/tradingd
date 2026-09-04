//! A startup demonstration of the quantizer against the filters just fetched.
//!
//! Off by default, behind `--quantize-demo`. It exists because milestone 3
//! builds a component that nothing yet calls: no order is placed until milestone
//! 6, so without this the only evidence the quantizer works against *real*
//! exchange filters would be a unit test using a captured fixture. This runs it
//! against the rules the live exchange returned a moment ago, and prints what it
//! decided.
//!
//! Every intent below is derived from the symbol's own filters rather than
//! hard-coded, so it demonstrates the same three outcomes for any symbol. None
//! of them is sent anywhere: `quantize` is pure, and there is no order path yet.

use domain::{
    quantize, ClientOrderId, Decimal, OrderIntent, OrderKind, Price, Qty, Side, Symbol,
    SymbolFilters, TimeInForce,
};

/// Log one accepted and two rejected quantizations for each symbol.
pub fn run(filters: &SymbolFilters) {
    let symbol = filters.symbol();
    let two = Decimal::from(2);

    // A reference price that is inside the price bounds and large enough that
    // the minimum quantity clears the minimum notional. Nothing to do with the
    // market - there is no market data at this point in startup.
    let reference = reference_price(filters);
    let tick = filters.tick_size().0;
    let step = filters.step_size().0;

    // 1. Accepted, with visible rounding on both fields: half a tick above a
    //    valid price floors back down for a buy, and a fraction of a step is
    //    floored off the quantity.
    let half_tick = tick.checked_div(two).unwrap_or(Decimal::ZERO);
    let half_step = step.checked_div(two).unwrap_or(Decimal::ZERO);
    report(
        "a plausible order, rounded to the exchange's grid",
        &intent(
            symbol,
            "demo-accept",
            Side::Buy,
            OrderKind::Limit {
                price: Price(reference + half_tick),
            },
            Qty(filters.min_qty().0 + filters.min_qty().0 + half_step),
        ),
        filters,
    );

    // 2. Rejected: dust. The minimum quantity at the minimum price is a real
    //    order shape, and on most symbols it is far under the minimum notional.
    report(
        "an order at the smallest allowed price and size",
        &intent(
            symbol,
            "demo-dust",
            Side::Buy,
            OrderKind::Limit {
                price: Price(if filters.min_price().0.is_zero() {
                    tick.max(Decimal::ONE)
                } else {
                    filters.min_price().0
                }),
            },
            Qty(filters.min_qty().0.max(step)),
        ),
        filters,
    );

    // 3. Rejected: market orders are milestone 6, and say so rather than being
    //    quietly mishandled.
    report(
        "a market order",
        &intent(
            symbol,
            "demo-market",
            Side::Buy,
            OrderKind::Market,
            Qty(filters.min_qty().0.max(step)),
        ),
        filters,
    );
}

/// A price inside the bounds at which `min_qty` clears `min_notional`.
fn reference_price(filters: &SymbolFilters) -> Decimal {
    let min_qty = filters.min_qty().0;
    let needed = if min_qty.is_zero() {
        filters.min_notional()
    } else {
        filters
            .min_notional()
            .checked_div(min_qty)
            .unwrap_or(Decimal::ONE)
    };
    let floor = needed.max(filters.min_price().0).max(Decimal::ONE);
    if filters.max_price().0.is_zero() {
        floor
    } else {
        floor.min(filters.max_price().0)
    }
}

fn intent(symbol: &Symbol, id: &str, side: Side, kind: OrderKind, qty: Qty) -> OrderIntent {
    OrderIntent {
        client_order_id: ClientOrderId(id.to_owned()),
        symbol: symbol.clone(),
        side,
        kind,
        qty,
        tif: TimeInForce::Gtc,
    }
}

fn report(what: &str, intent: &OrderIntent, filters: &SymbolFilters) {
    let requested = match intent.kind {
        OrderKind::Limit { price } => price.to_string(),
        OrderKind::Market => "market".to_owned(),
    };

    match quantize(intent, filters) {
        Ok(order) => tracing::info!(
            symbol = %order.symbol(),
            what,
            requested_price = requested,
            requested_qty = %intent.qty,
            price = %order.price(),
            qty = %order.qty(),
            notional = %order.notional(),
            "quantize demo: accepted"
        ),
        Err(reject) => tracing::info!(
            symbol = %intent.symbol,
            what,
            requested_price = requested,
            requested_qty = %intent.qty,
            reason = %reject,
            "quantize demo: rejected"
        ),
    }
}
