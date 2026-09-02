//! The seam between a market source and the engine.
//!
//! [`IngestMsg`] is what flows down the channel from whatever is producing
//! market data - the live Binance socket today, a replay source later - into the
//! engine's run loop. It is deliberately a *channel of plain data* rather than a
//! trait: `bot` injects a concrete source the same way it injects a concrete
//! strategy, and the engine never learns which one it got.
//!
//! There is no `MarketSource` trait yet, on purpose. An abstraction drawn around
//! one implementation is a guess; when the replay source arrives there will be
//! two real cases to draw it around.
//!
//! Note what is *not* here: no serde, no async, no exchange types. This crate
//! still depends on nothing but `rust_decimal` and `thiserror`. Recording
//! serialises raw exchange payloads over in the adapter, so these types never
//! need to be a wire format.

use crate::types::MarketEvent;

/// One message from a market source.
///
/// Health facts travel in the same ordered channel as the data they describe,
/// rather than in a side channel. That ordering is the point: a consumer sees
/// `Gap` immediately before the first message on the far side of it, and a
/// replay of a recording reproduces that same interleaving, because the
/// recording stores the markers inline for exactly this reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestMsg {
    /// A normalized market event.
    Market(MarketEvent),

    /// Messages are missing on `stream`, or its sequence went backwards.
    ///
    /// `detail` is human-readable text rather than a structured value, and that
    /// is a deliberate limitation. What a gap *means* depends on the sequence
    /// semantics of the particular stream - a countable run of lost trades is
    /// not the same fact as an order-book id that merely jumped - and those
    /// semantics are exchange knowledge. Teaching this crate about them would
    /// drag Binance's sequencing rules into the pure domain.
    ///
    /// So the adapter, which owns the semantics, renders them into a string that
    /// always names both the policy that applied and the kind of evidence found.
    /// The structured form is preserved where it has to survive: in the
    /// recording, which is the durable artefact a replay reads.
    Gap { stream: String, detail: String },

    /// No message has arrived on `stream` for longer than the configured
    /// staleness bound. `since_ns` is the ingest time of the last thing we did
    /// hear from it, or of the moment we connected if it has never spoken.
    Stale { stream: String, since_ns: i64 },

    /// The source established (or re-established) its connection.
    Connected,

    /// The source lost its connection. Reconnection is the source's business;
    /// this is a report, not a request.
    Disconnected { reason: String },
}
