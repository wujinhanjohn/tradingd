//! The Binance adapter: market-data ingest, normalization, and recording.
//!
//! Milestone 2 is **read-only**. There is no signing, no order placement, and no
//! HTTP client - public market streams are unauthenticated WebSocket, and that
//! is all this crate reaches for. `reqwest`/`hyper`/`ureq` are deliberately
//! absent from the workspace; TLS arrives only as `rustls`, transitively, because
//! `wss://` requires it.
//!
//! Dependency direction: `exchange -> domain` plus external crates. It does
//! **not** depend on `settings`, so the environment cross-check in [`require_class`]
//! is expressed in this crate's own [`EndpointClass`] and `bot` does the one-line
//! mapping from `settings::Env`.

mod endpoint;
mod gap;
mod normalize;
mod record;

pub use endpoint::{
    classify, is_loopback, require_class, EndpointClass, EndpointError, PRODUCTION_HOSTS,
    PRODUCTION_SPOT_WS_URL, TESTNET_HOSTS, TESTNET_SPOT_WS_URL,
};
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
