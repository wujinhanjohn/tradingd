use std::sync::Arc;

use rust_decimal::Decimal;

use crate::error::DomainError;

// --- primitives ---

/// A price, always exact decimal. Never a float.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub Decimal);

/// A quantity in base asset units, always exact decimal. Never a float.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Qty(pub Decimal);

/// An exchange trading pair, e.g. `"BTCUSDT"`.
///
/// Cheap to clone and hash: strategies and books pass these around constantly.
///
/// The inner field is private on purpose: [`Symbol::new`] is the only way to
/// build one, so validation is an invariant rather than a convention.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Symbol(Arc<str>);

/// Exchange event time, milliseconds since Unix epoch.
pub type Timestamp = i64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderKind {
    Market,
    Limit { price: Price },
}

/// We generate this ourselves. It is the idempotency key: retrying an order
/// with the same id must never create a second order.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientOrderId(pub String);

// --- what a strategy asks the engine to do ---

#[derive(Clone, Debug)]
pub struct OrderIntent {
    pub client_order_id: ClientOrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub kind: OrderKind,
    /// DESIRED size, pre-quantization. The quantizer (M3) rounds this to a valid
    /// exchange step size before send.
    pub qty: Qty,
    pub tif: TimeInForce,
}

#[derive(Clone, Debug)]
pub enum Action {
    Place(OrderIntent),
    Cancel {
        symbol: Symbol,
        client_order_id: ClientOrderId,
    },
}

// --- what the engine feeds a strategy (grows over time) ---

#[derive(Clone, Debug)]
pub struct BookTicker {
    pub symbol: Symbol,
    pub bid: Price,
    pub bid_qty: Qty,
    pub ask: Price,
    pub ask_qty: Qty,
    pub event_time: Timestamp,
}

#[derive(Clone, Debug)]
pub struct Trade {
    pub symbol: Symbol,
    pub price: Price,
    pub qty: Qty,
    pub event_time: Timestamp,
}

#[derive(Clone, Debug)]
pub enum MarketEvent {
    BookTicker(BookTicker),
    Trade(Trade),
    // Depth, Kline, etc. added in later milestones.
}

#[derive(Clone, Debug)]
pub struct Fill {
    pub client_order_id: ClientOrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub fee: Decimal,
    pub event_time: Timestamp,
}

// --- construction helpers ---

impl Symbol {
    /// Validate and construct a symbol.
    ///
    /// Fails closed: only non-empty, uppercase ASCII alphanumeric strings are
    /// accepted, because that is the whole of what Binance spot uses. A symbol
    /// we cannot vouch for is rejected rather than passed through to an order.
    ///
    /// This is the only constructor; the inner field is private.
    pub fn new(raw: &str) -> Result<Self, DomainError> {
        let valid = !raw.is_empty()
            && raw.len() <= 32
            && raw
                .bytes()
                .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase());
        if valid {
            Ok(Self(Arc::from(raw)))
        } else {
            Err(DomainError::InvalidSymbol(raw.to_owned()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Side {
    /// The opposite side. Handy for close/flatten logic in later milestones.
    #[must_use]
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn symbol_accepts_uppercase_alphanumeric() {
        let s = Symbol::new("BTCUSDT").expect("valid symbol");
        assert_eq!(s.as_str(), "BTCUSDT");
        assert_eq!(s.to_string(), "BTCUSDT");
        assert!(Symbol::new("1INCHUSDT").is_ok());
    }

    #[test]
    fn symbol_rejects_anything_it_cannot_vouch_for() {
        for bad in [
            "",
            "btcusdt",
            "BTC-USDT",
            "BTC USDT",
            "BTC/USDT",
            "BTCUSDT\n",
        ] {
            let err = Symbol::new(bad).expect_err("should reject");
            let DomainError::InvalidSymbol(reported) = &err;
            assert_eq!(reported, bad, "error should echo the offending input");
        }
    }

    #[test]
    fn prices_compare_exactly_without_float_error() {
        // The canonical 0.1 + 0.2 != 0.3 float trap. Decimal must not have it.
        assert_eq!(Price(dec!(0.1).saturating_add(dec!(0.2))), Price(dec!(0.3)));
        assert!(Price(dec!(100.00)) < Price(dec!(100.01)));
        // Decimal equality ignores trailing-zero scale differences.
        assert_eq!(Qty(dec!(1.50)), Qty(dec!(1.5)));
    }

    #[test]
    fn side_opposite_round_trips() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite().opposite(), Side::Sell);
    }

    #[test]
    fn order_intent_carries_a_limit_price() {
        let intent = OrderIntent {
            client_order_id: ClientOrderId("abc-1".to_owned()),
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            side: Side::Buy,
            kind: OrderKind::Limit {
                price: Price(dec!(65000.10)),
            },
            qty: Qty(dec!(0.001)),
            tif: TimeInForce::Gtc,
        };
        let OrderKind::Limit { price } = intent.kind else {
            panic!("expected a limit order");
        };
        assert_eq!(price, Price(dec!(65000.10)));
    }
}
