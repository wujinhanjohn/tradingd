//! Strategy implementations.
//!
//! Milestone 1 ships only [`NoopStrategy`], which exists to prove the
//! [`domain::Strategy`] seam holds: the engine can own a `Box<dyn Strategy>` and
//! drive it without knowing anything about what it does. Real strategies drop in
//! here later with no engine changes.

use domain::{Action, MarketEvent, Strategy, StrategyCtx};

/// A strategy that observes everything and does nothing.
///
/// The safest possible default, and the one the bot runs with in milestone 1:
/// it can never emit an order, because it never returns an [`Action`].
pub struct NoopStrategy;

impl Strategy for NoopStrategy {
    fn name(&self) -> &str {
        "noop"
    }

    fn on_market(&mut self, _event: &MarketEvent, _ctx: &StrategyCtx<'_>) -> Vec<Action> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use domain::{BookTicker, ClientOrderId, Fill, Price, Qty, Side, Symbol, Timestamp, Trade};
    use rust_decimal_macros::dec;

    use super::*;

    const NOW: Timestamp = 1_700_000_000_000;

    fn symbol() -> Symbol {
        Symbol::new("BTCUSDT").expect("valid symbol")
    }

    fn book_ticker() -> MarketEvent {
        MarketEvent::BookTicker(BookTicker {
            symbol: symbol(),
            bid: Price(dec!(64999.99)),
            bid_qty: Qty(dec!(1.5)),
            ask: Price(dec!(65000.01)),
            ask_qty: Qty(dec!(2.0)),
            event_time: NOW,
        })
    }

    fn trade() -> MarketEvent {
        MarketEvent::Trade(Trade {
            symbol: symbol(),
            price: Price(dec!(65000)),
            qty: Qty(dec!(0.01)),
            event_time: NOW,
        })
    }

    fn fill() -> Fill {
        Fill {
            client_order_id: ClientOrderId("noop-1".to_owned()),
            symbol: symbol(),
            side: Side::Buy,
            price: Price(dec!(65000)),
            qty: Qty(dec!(0.001)),
            fee: dec!(0.0000075),
            event_time: NOW,
        }
    }

    #[test]
    fn reports_its_name() {
        assert_eq!(NoopStrategy.name(), "noop");
    }

    #[test]
    fn never_emits_an_action_for_any_market_event() {
        let mut s = NoopStrategy;
        let ctx = StrategyCtx::new(NOW);
        for event in [book_ticker(), trade()] {
            assert!(
                s.on_market(&event, &ctx).is_empty(),
                "the no-op strategy must never place an order"
            );
        }
    }

    #[test]
    fn never_emits_an_action_on_a_fill() {
        // NoopStrategy does not override `on_fill`; this also pins the trait's
        // default implementation to "do nothing".
        let mut s = NoopStrategy;
        assert!(s.on_fill(&fill(), &StrategyCtx::new(NOW)).is_empty());
    }

    #[test]
    fn stays_silent_across_a_long_run_of_events() {
        // The engine will drive this in a loop; repeated calls must not
        // accumulate state that eventually produces an action.
        let mut s = NoopStrategy;
        let ctx = StrategyCtx::new(NOW);
        let event = trade();
        for _ in 0..1_000 {
            assert!(s.on_market(&event, &ctx).is_empty());
        }
    }

    #[test]
    fn works_through_the_boxed_trait_object_the_engine_holds() {
        // This is exactly how `bot` and `Engine` will hold a strategy.
        let mut s: Box<dyn Strategy> = Box::new(NoopStrategy);
        assert_eq!(s.name(), "noop");
        assert!(s
            .on_market(&book_ticker(), &StrategyCtx::new(NOW))
            .is_empty());
        assert!(s.on_fill(&fill(), &StrategyCtx::new(NOW)).is_empty());
    }

    #[test]
    fn is_send_so_the_engine_can_own_it_across_tasks() {
        fn assert_send<T: Send>(_: &T) {}
        assert_send(&NoopStrategy);
        let boxed: Box<dyn Strategy> = Box::new(NoopStrategy);
        assert_send(&boxed);
    }
}
