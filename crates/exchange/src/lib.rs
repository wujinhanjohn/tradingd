//! The Binance adapter: market-data ingest, normalization, recording, and the
//! symbol filters an order has to satisfy.
//!
//! Still **read-only**. There is no signing and no order placement: public
//! market streams are unauthenticated WebSocket, and `exchangeInfo` is an
//! unauthenticated GET. Milestone 3 retired the "no HTTP client" property and
//! replaced it with a stricter one - there is exactly **one** HTTP client
//! (`ureq`, on rustls with the `ring` provider), it is [`RestClient`], and every
//! request goes through it. `reqwest`/`hyper`/`native-tls`/`aws-lc-rs` stay out
//! of the workspace entirely.
//!
//! Dependency direction: `exchange -> domain` plus external crates. It does
//! **not** depend on `settings`, so the environment cross-check in [`require_class`]
//! is expressed in this crate's own [`EndpointClass`] and `bot` does the one-line
//! mapping from `settings::Env`.

mod backoff;
mod binance;
mod book;
mod endpoint;
mod filters;
mod gap;
mod normalize;
mod record;
mod rest;
mod source;
mod subscription;
mod wire;

pub use backoff::{Backoff, BackoffError, Jitter};
pub use binance::ConnectError;
pub use book::{FilterBook, FilterStale};
pub use endpoint::{
    classify, classify_for, is_loopback, is_loopback_for, require_class, require_class_for,
    EndpointClass, EndpointError, Protocol, PRODUCTION_HOSTS, PRODUCTION_SPOT_REST_URL,
    PRODUCTION_SPOT_WS_URL, TESTNET_HOSTS, TESTNET_SPOT_REST_URL, TESTNET_SPOT_WS_URL,
};
pub use filters::{parse_exchange_info, FilterParseError, LotSize, SymbolInfo, STATUS_TRADING};
pub use gap::{GapDetail, GapKind, SeqTracker};
pub use normalize::{
    ingest_ms, normalize, parse_stream, NormalizeError, Normalized, Seq, SeqPolicy, StreamId,
    StreamKind,
};
pub use record::{
    open_session, read_session, session_filename, DataRecord, DisconnectDetail, Header, Marker,
    MarkerKind, MarkerRecord, ReconnectDetail, Record, RecordError, RecordReader, Recorder,
    RecorderConfig, EXTENSION, FORMAT, FORMAT_VERSION,
};
pub use rest::{ExchangeInfo, FilterRefresher, RestClient, RestError};
pub use source::{BinanceMarketSource, Clock, SourceConfig, SourceError, SystemClock};
pub use subscription::{StreamSet, StreamTracker, Subscription, SubscriptionError, TrackError};
pub use wire::{parse_frame, subscribe_request, Frame, WireError};
