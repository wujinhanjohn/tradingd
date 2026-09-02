//! Pure trading domain: primitives, market events, order intents, and the
//! [`Strategy`] seam that every strategy implementation plugs into.
//!
//! This crate is deliberately free of I/O, async runtimes, and exchange-specific
//! types. It depends on nothing internal, and only on `rust_decimal` and
//! `thiserror` externally, so that the live engine and the backtest harness can
//! both build on exactly the same domain.
//!
//! Money is always [`rust_decimal::Decimal`]. Binary floating point is banned.

/// Re-exported so downstream crates construct domain values with exactly the
/// `Decimal` this crate was built against, rather than a second, incompatible
/// version of `rust_decimal` resolved on their own.
pub use rust_decimal::Decimal;

mod error;
mod ingest;
mod strategy;
mod types;

pub use error::DomainError;
pub use ingest::IngestMsg;
pub use strategy::{Strategy, StrategyCtx};
pub use types::{
    Action, BookTicker, ClientOrderId, Fill, MarketEvent, OrderIntent, OrderKind, Price, Qty, Side,
    Symbol, TimeInForce, Timestamp, Trade,
};
