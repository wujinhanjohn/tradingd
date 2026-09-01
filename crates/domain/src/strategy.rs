use crate::types::{Action, Fill, MarketEvent, Timestamp};

/// Read-only snapshot handed to the strategy on each callback. Will gain
/// positions, open orders, and a clock in later milestones.
///
/// The private field keeps the struct non-exhaustive from outside this crate,
/// so later milestones can add fields without breaking every call site.
#[derive(Clone, Copy, Debug)]
pub struct StrategyCtx<'a> {
    pub now: Timestamp,
    _priv: std::marker::PhantomData<&'a ()>,
}

impl StrategyCtx<'_> {
    /// Build a context for a callback. Only the engine and the backtest harness
    /// should need this.
    #[must_use]
    pub fn new(now: Timestamp) -> Self {
        Self {
            now,
            _priv: std::marker::PhantomData,
        }
    }
}

/// The seam. Everything downstream is built so a real strategy drops in here
/// later with no engine changes.
///
/// Implementations are driven from the engine task, so they must be `Send`.
/// They are called with `&mut self` and must not block: a strategy returns
/// [`Action`]s and lets the engine own all I/O.
pub trait Strategy: Send {
    fn name(&self) -> &str;

    fn on_market(&mut self, event: &MarketEvent, ctx: &StrategyCtx<'_>) -> Vec<Action>;

    fn on_fill(&mut self, fill: &Fill, ctx: &StrategyCtx<'_>) -> Vec<Action> {
        let _ = (fill, ctx);
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;
    use crate::types::{ClientOrderId, Fill, Price, Qty, Side, Symbol, Trade};

    struct Counter {
        market: usize,
        fills: usize,
    }

    impl Strategy for Counter {
        fn name(&self) -> &str {
            "counter"
        }

        fn on_market(&mut self, _event: &MarketEvent, _ctx: &StrategyCtx<'_>) -> Vec<Action> {
            self.market += 1;
            Vec::new()
        }
    }

    fn symbol() -> Symbol {
        Symbol::new("BTCUSDT").expect("valid symbol")
    }

    #[test]
    fn strategy_is_object_safe_and_dispatches() {
        let mut s: Box<dyn Strategy> = Box::new(Counter {
            market: 0,
            fills: 0,
        });
        let ctx = StrategyCtx::new(1_700_000_000_000);
        let event = MarketEvent::Trade(Trade {
            symbol: symbol(),
            price: Price(dec!(65000)),
            qty: Qty(dec!(0.01)),
            event_time: ctx.now,
        });

        assert_eq!(s.name(), "counter");
        assert!(s.on_market(&event, &ctx).is_empty());
        assert!(s.on_market(&event, &ctx).is_empty());
    }

    #[test]
    fn on_fill_defaults_to_doing_nothing() {
        let mut s = Counter {
            market: 0,
            fills: 0,
        };
        let ctx = StrategyCtx::new(42);
        let fill = Fill {
            client_order_id: ClientOrderId("abc-1".to_owned()),
            symbol: symbol(),
            side: Side::Buy,
            price: Price(dec!(65000)),
            qty: Qty(dec!(0.001)),
            fee: dec!(0.0000075),
            event_time: 42,
        };

        // Default impl: no actions, and it does not touch the strategy's state.
        assert!(s.on_fill(&fill, &ctx).is_empty());
        assert_eq!(s.fills, 0);
    }

    #[test]
    fn ctx_carries_the_callback_time() {
        assert_eq!(StrategyCtx::new(1_234).now, 1_234);
    }
}
