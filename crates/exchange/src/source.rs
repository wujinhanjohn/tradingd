//! The live market source: the thing `bot` spawns, and the receiver the engine
//! reads.
//!
//! # What this module is, and what it deliberately is not
//!
//! It is a driver. Every judgement it makes is made by a module that can be
//! tested without a socket: [`crate::endpoint`] decides whether the URL may be
//! connected to at all, [`crate::normalize`] turns bytes into events,
//! [`crate::subscription`] decides what a gap means on a given stream,
//! [`crate::gap`] finds them, [`crate::record`] writes them down, and
//! [`crate::backoff`] paces the retries. This module owns none of that logic; it
//! owns the order things happen in.
//!
//! There is no `MarketSource` trait. The seam between a source and the engine is
//! a channel of [`domain::IngestMsg`], and an abstraction drawn around a single
//! implementation would be a guess. When the replay source arrives there will be
//! two real cases to draw it around.
//!
//! # The endpoint check is structural
//!
//! [`BinanceMarketSource::connect`] calls [`require_class`] before it does
//! anything else, and it is synchronous: it opens no socket, and on a mismatch
//! it returns without producing a source at all. A testnet-labelled
//! configuration aimed at a production host does not connect-and-warn, it fails
//! to exist. The socket is opened later, by [`BinanceMarketSource::run`], which
//! can only be called on a value that passed the check.
//!
//! # Ordering, and why it matches the recording exactly
//!
//! When a message evidences a gap, the gap marker is written *before* the
//! payload and [`domain::IngestMsg::Gap`] is emitted *before* the
//! [`domain::IngestMsg::Market`] it describes. Replay reads the file in order
//! and produces the same interleaving. That is the whole reason markers are
//! stored inline rather than in a sidecar: live and replay must agree about what
//! a consumer saw, and when.
//!
//! Staleness is the one health signal that is emitted but not recorded, because
//! it is derived rather than observed: a reader with the recording's `recv_ns`
//! timeline and the configured bound recomputes it exactly. A gap is not like
//! that - it depends on the sequence policy that applied at capture time - which
//! is why that one is written down.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use domain::IngestMsg;
use serde_json::value::RawValue;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use crate::backoff::{Backoff, Jitter};
use crate::binance::{self, ConnectError, Socket};
use crate::endpoint::{require_class, EndpointClass, EndpointError};
use crate::gap::GapDetail;
use crate::normalize::{normalize, SeqPolicy};
use crate::record::{
    DisconnectDetail, Marker, ReconnectDetail, RecordError, Recorder, RecorderConfig,
};
use crate::subscription::{StreamSet, StreamTracker};
use crate::wire::{self, Frame};

/// Our ingest clock.
///
/// A trait rather than a direct `SystemTime::now()` for the same reason
/// [`normalize`] takes `recv_ns` as a parameter: the timestamp is the one piece
/// of non-determinism on the ingest path, so it enters through a seam that a
/// test can hold still. Production uses [`SystemClock`] and nothing else.
pub trait Clock: std::fmt::Debug + Send + Sync {
    /// Nanoseconds since the Unix epoch.
    fn now_ns(&self) -> i64;
}

/// The wall clock. The only implementation used outside tests.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_nanos()).ok())
            // A clock before the epoch, or past the year 2262, is a broken
            // clock. Zero is refused downstream by `normalize::ingest_ms`'s
            // sibling checks and by `Recorder::create`, so a bad clock stops the
            // session rather than silently stamping nonsense into the archive.
            .unwrap_or(0)
    }
}

/// How the source should behave. Everything here is settled before a socket
/// exists.
#[derive(Clone, Debug)]
pub struct SourceConfig {
    /// What to subscribe to. Non-empty and duplicate-free by construction.
    pub streams: StreamSet,
    /// Silence on a subscribed stream longer than this emits
    /// [`IngestMsg::Stale`].
    pub staleness: Duration,
    /// Where to write the session recording, or `None` to run unrecorded.
    ///
    /// `Some` is fail-closed: an unwritable directory refuses to start.
    pub recording_dir: Option<PathBuf>,
    /// Reconnect pacing.
    pub backoff: Backoff,
    /// How long the WebSocket handshake may take before the attempt is abandoned.
    pub connect_timeout: Duration,
    /// How long to wait for the exchange to acknowledge our SUBSCRIBE before
    /// treating the connection as no good.
    pub ack_timeout: Duration,
    /// Channel depth between the source and the engine.
    pub channel_capacity: usize,
}

impl SourceConfig {
    /// Sensible defaults around a subscription set.
    #[must_use]
    pub fn new(streams: StreamSet) -> Self {
        Self {
            streams,
            staleness: Duration::from_secs(10),
            recording_dir: None,
            backoff: Backoff::default_schedule(),
            connect_timeout: Duration::from_secs(10),
            ack_timeout: Duration::from_secs(10),
            channel_capacity: 1_024,
        }
    }
}

/// Every reason a source refuses to start, or stops for good.
///
/// Note what is *not* here: a dropped connection. That is normal operation and
/// is handled by reconnecting. These are the failures that end the source, which
/// closes the channel, which shuts the engine down - the fail-closed path.
#[derive(thiserror::Error, Debug)]
pub enum SourceError {
    #[error(transparent)]
    Endpoint(#[from] EndpointError),

    #[error(transparent)]
    Recording(#[from] RecordError),

    #[error("staleness bound must be at least 1ms")]
    ZeroStaleness,

    #[error("channel capacity must be at least 1")]
    ZeroCapacity,

    #[error(
        "the exchange refused our subscription to {streams:?}: code {code}, {msg}. \
         Retrying would produce a bot that looks alive and receives nothing, so \
         the source stops instead"
    )]
    SubscriptionRejected {
        streams: Vec<String>,
        code: i64,
        msg: String,
    },
}

/// A validated, read-only Binance market-data source.
///
/// Constructing one proves the endpoint matches the declared environment and
/// that the recording target is writable. Running one opens the socket.
#[derive(Debug)]
pub struct BinanceMarketSource {
    ws_url: String,
    endpoint: EndpointClass,
    streams: StreamSet,
    staleness: Duration,
    connect_timeout: Duration,
    ack_timeout: Duration,
    backoff: Backoff,
    jitter: Jitter,
    clock: Box<dyn Clock>,
    /// Survives reconnects: a gap spanning an outage is reported against the
    /// last id seen before it, which is state the previous session left behind.
    tracker: StreamTracker,
    recorder: Option<Recorder>,
    tx: mpsc::Sender<IngestMsg>,
}

impl BinanceMarketSource {
    /// Validate everything that can be validated without a network, and hand
    /// back the source plus the receiver the engine reads.
    ///
    /// Opens **no socket**. The endpoint check runs first, so a source aimed at
    /// the wrong environment is never constructed - there is no object to call
    /// [`BinanceMarketSource::run`] on. The recorder is created here too, for
    /// the same reason: an unwritable recording directory must stop the process
    /// at startup, not at the first message, by which time an operator believes
    /// the session is being captured.
    ///
    /// # Errors
    ///
    /// [`SourceError::Endpoint`] when `ws_url` does not belong to `expected`,
    /// [`SourceError::Recording`] when recording is enabled and the target is
    /// unusable, and the shape errors for a nonsensical configuration.
    pub fn connect(
        expected: EndpointClass,
        ws_url: &str,
        config: SourceConfig,
    ) -> Result<(Self, mpsc::Receiver<IngestMsg>), SourceError> {
        Self::connect_with_clock(expected, ws_url, config, Box::new(SystemClock))
    }

    /// [`BinanceMarketSource::connect`] with the ingest clock injected.
    ///
    /// Exists so tests can assert on exact recorded timestamps. Production calls
    /// [`BinanceMarketSource::connect`].
    ///
    /// # Errors
    ///
    /// As [`BinanceMarketSource::connect`].
    pub fn connect_with_clock(
        expected: EndpointClass,
        ws_url: &str,
        config: SourceConfig,
        clock: Box<dyn Clock>,
    ) -> Result<(Self, mpsc::Receiver<IngestMsg>), SourceError> {
        // First, before anything else, and before any I/O at all.
        require_class(expected, ws_url)?;

        if config.staleness.is_zero() {
            return Err(SourceError::ZeroStaleness);
        }
        if config.channel_capacity == 0 {
            return Err(SourceError::ZeroCapacity);
        }

        let recorder = config
            .recording_dir
            .as_ref()
            .map(|dir| {
                Recorder::create(&RecorderConfig {
                    dir: dir.clone(),
                    started_ns: clock.now_ns(),
                    endpoint: expected,
                    ws_url: ws_url.to_owned(),
                    symbols: config.streams.symbols(),
                    streams: config.streams.names(),
                })
            })
            .transpose()?;

        let (tx, rx) = mpsc::channel(config.channel_capacity);

        Ok((
            Self {
                ws_url: ws_url.to_owned(),
                endpoint: expected,
                tracker: StreamTracker::new(&config.streams),
                streams: config.streams,
                staleness: config.staleness,
                connect_timeout: config.connect_timeout,
                ack_timeout: config.ack_timeout,
                backoff: config.backoff,
                jitter: Jitter::from_entropy(),
                clock,
                recorder,
                tx,
            },
            rx,
        ))
    }

    /// Seed the reconnect jitter, so a test gets a repeatable schedule.
    #[must_use]
    pub fn with_jitter_seed(mut self, seed: u64) -> Self {
        self.jitter = Jitter::from_seed(seed);
        self
    }

    /// Where the session recording is being written, if it is.
    #[must_use]
    pub fn recording_path(&self) -> Option<&std::path::Path> {
        self.recorder.as_ref().map(Recorder::path)
    }

    /// Connect, subscribe, and pump until the consumer goes away or something
    /// fatal happens.
    ///
    /// Dropped connections are not fatal - they are expected, since a Binance
    /// connection is only valid for 24 hours - and are retried under
    /// [`Backoff`], which is capped and jittered so this can never become a hot
    /// loop against the exchange.
    ///
    /// Returns when the receiver is dropped, which is how the engine says it is
    /// shutting down. The recording is flushed before returning either way.
    ///
    /// # Errors
    ///
    /// [`SourceError`] for the failures that must stop the source: a refused
    /// subscription, or a recording that can no longer be written. Both close
    /// the channel, which the engine treats as a reason to shut down.
    pub async fn run(mut self) -> Result<(), SourceError> {
        tracing::info!(
            url = self.ws_url,
            endpoint = %self.endpoint,
            streams = ?self.streams.names(),
            staleness_ms = self.staleness.as_millis(),
            recording = ?self.recording_path(),
            "market source starting"
        );

        let mut attempt: u32 = 0;
        let mut waited_ms: u64 = 0;

        let outcome = loop {
            if self.tx.is_closed() {
                break Ok(());
            }

            match self.session(attempt, waited_ms).await {
                Ok(SessionEnd::Shutdown) => break Ok(()),
                Ok(SessionEnd::Ended(reason)) => {
                    if self.disconnected(&reason).await? == Flow::Shutdown {
                        break Ok(());
                    }
                }
                Err(fatal) => break Err(fatal),
            }

            attempt = attempt.saturating_add(1);
            let delay = self.backoff.delay(attempt, &mut self.jitter);
            waited_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
            tracing::warn!(
                attempt,
                wait_ms = waited_ms,
                "reconnecting to the market stream"
            );

            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = self.tx.closed() => break Ok(()),
            }
        };

        self.finish(outcome)
    }

    /// Flush and close the recording, then report.
    ///
    /// A failed final flush is reported even when the session itself ended
    /// cleanly: silently losing the tail of an archive is the failure this crate
    /// exists to prevent.
    fn finish(mut self, outcome: Result<(), SourceError>) -> Result<(), SourceError> {
        let flushed = self
            .recorder
            .take()
            .map(Recorder::finish)
            .transpose()
            .map(|path| {
                if let Some(path) = &path {
                    tracing::info!(recording = %path.display(), "session recording closed");
                }
            });

        match (outcome, flushed) {
            (Err(fatal), _) => {
                tracing::error!(error = %fatal, "market source stopped");
                Err(fatal)
            }
            (Ok(()), Err(record)) => {
                tracing::error!(error = %record, "market source could not close its recording");
                Err(record.into())
            }
            (Ok(()), Ok(())) => {
                tracing::info!("market source stopped cleanly");
                Ok(())
            }
        }
    }

    /// One connection, from handshake to drop.
    async fn session(&mut self, attempt: u32, waited_ms: u64) -> Result<SessionEnd, SourceError> {
        let names = self.streams.names();

        // The handshake races the consumer going away. Without this, a Ctrl-C
        // arriving while a reconnect attempt is in flight would sit behind the
        // whole `connect_timeout` before the process could stop - a real, if
        // bounded, hang on the shutdown path.
        let opened = tokio::select! {
            biased;
            () = self.tx.closed() => return Ok(SessionEnd::Shutdown),
            result = binance::open(&self.ws_url, &names, self.connect_timeout) => result,
        };

        let mut socket = match opened {
            Ok(socket) => socket,
            Err(error) => return Ok(SessionEnd::Ended(describe_connect(&error))),
        };

        if attempt > 0 {
            self.record(Marker::Reconnect(ReconnectDetail { attempt, waited_ms }))?;
        }

        tracing::info!(url = self.ws_url, attempt, "connected to the market stream");
        if self.emit(IngestMsg::Connected).await == Flow::Shutdown {
            binance::close(socket).await;
            return Ok(SessionEnd::Shutdown);
        }

        let started = Instant::now();
        let mut staleness =
            Staleness::new(self.staleness, &self.streams, started, self.clock.now_ns());
        let mut ack_deadline = Some(started + self.ack_timeout);

        let end = loop {
            let wakeup = tokio::select! {
                biased;
                () = self.tx.closed() => Wakeup::Shutdown,
                () = maybe_sleep(ack_deadline) => Wakeup::AckOverdue,
                () = maybe_sleep(staleness.next_deadline()) => Wakeup::Staleness,
                message = binance::next_message(&mut socket) => Wakeup::Message(message),
            };

            match wakeup {
                Wakeup::Shutdown => break SessionEnd::Shutdown,

                Wakeup::AckOverdue => {
                    break SessionEnd::Ended(format!(
                        "the exchange did not acknowledge our subscription within {}s",
                        self.ack_timeout.as_secs()
                    ))
                }

                Wakeup::Staleness => {
                    for (stream, since_ns) in staleness.take_due(Instant::now()) {
                        tracing::error!(
                            stream,
                            since_ns,
                            bound_ms = self.staleness.as_millis(),
                            "stream has gone silent"
                        );
                        if self.emit(IngestMsg::Stale { stream, since_ns }).await == Flow::Shutdown
                        {
                            break;
                        }
                    }
                    if self.tx.is_closed() {
                        break SessionEnd::Shutdown;
                    }
                }

                Wakeup::Message(None) => {
                    break SessionEnd::Ended("the exchange closed the connection".to_owned())
                }

                Wakeup::Message(Some(Err(error))) => {
                    break SessionEnd::Ended(format!("socket error: {error}"))
                }

                Wakeup::Message(Some(Ok(message))) => {
                    match self
                        .on_message(&mut socket, message, &mut staleness, &mut ack_deadline)
                        .await?
                    {
                        Step::Continue => {}
                        Step::Shutdown => break SessionEnd::Shutdown,
                        Step::Ended(reason) => break SessionEnd::Ended(reason),
                    }
                }
            }
        };

        if matches!(end, SessionEnd::Shutdown) {
            binance::close(socket).await;
        }
        Ok(end)
    }

    /// One inbound WebSocket message.
    async fn on_message(
        &mut self,
        socket: &mut Socket,
        message: Message,
        staleness: &mut Staleness,
        ack_deadline: &mut Option<Instant>,
    ) -> Result<Step, SourceError> {
        match message {
            Message::Text(text) => self.on_text(text.as_str(), staleness, ack_deadline).await,

            Message::Ping(_) => {
                // Do not write a pong: tungstenite has already queued one
                // carrying this ping's payload, and a custom pong would replace
                // it rather than accompany it. Flushing is what actually puts it
                // on the wire, and the docs are explicit that only a pong
                // echoing the ping's payload counts as keepalive.
                match binance::answer_ping(socket).await {
                    Ok(()) => {
                        tracing::trace!("answered a server ping");
                        Ok(Step::Continue)
                    }
                    Err(error) => Ok(Step::Ended(format!("could not answer a ping: {error}"))),
                }
            }

            Message::Close(frame) => Ok(Step::Ended(match frame {
                Some(frame) => format!("the exchange closed the connection: {frame}"),
                None => "the exchange closed the connection".to_owned(),
            })),

            Message::Pong(_) => Ok(Step::Continue),

            Message::Binary(bytes) => {
                // Market streams are text. Binary would mean we are pointed at
                // an SBE endpoint we did not ask for, so it is reported rather
                // than silently discarded.
                tracing::warn!(bytes = bytes.len(), "ignoring an unexpected binary frame");
                Ok(Step::Continue)
            }

            Message::Frame(_) => Ok(Step::Continue),
        }
    }

    /// One inbound text frame.
    async fn on_text(
        &mut self,
        text: &str,
        staleness: &mut Staleness,
        ack_deadline: &mut Option<Instant>,
    ) -> Result<Step, SourceError> {
        match wire::parse_frame(text) {
            Err(error) => {
                tracing::warn!(%error, "dropping an unreadable frame");
                Ok(Step::Continue)
            }

            Ok(Frame::Ack { id }) => {
                if id == binance::SUBSCRIBE_REQUEST_ID {
                    *ack_deadline = None;
                    tracing::info!(
                        streams = ?self.streams.names(),
                        "subscription acknowledged"
                    );
                } else {
                    tracing::warn!(id, "ignoring a response to a request we did not send");
                }
                Ok(Step::Continue)
            }

            Ok(Frame::Rejected { id, code, msg }) => {
                // Fatal, deliberately. A refused subscription does not fix
                // itself, and retrying it forever produces a process that is up,
                // connected, and receiving nothing.
                tracing::error!(?id, code, msg, "the exchange refused our subscription");
                Err(SourceError::SubscriptionRejected {
                    streams: self.streams.names(),
                    code,
                    msg,
                })
            }

            Ok(Frame::Other) => {
                tracing::warn!(frame = text, "unrecognised control frame");
                Ok(Step::Continue)
            }

            Ok(Frame::Data { stream, payload }) => {
                let stream = stream.to_owned();
                self.on_data(&stream, payload, staleness).await
            }
        }
    }

    /// One market payload: record it, normalize it, judge it, emit it.
    ///
    /// The raw payload is recorded on **every** path, including the ones where
    /// we cannot make sense of it. That is the point of recording raw: a later
    /// fix to [`normalize`] re-derives the right events from history, which it
    /// cannot do for a message we declined to write down.
    async fn on_data(
        &mut self,
        stream: &str,
        payload: &RawValue,
        staleness: &mut Staleness,
    ) -> Result<Step, SourceError> {
        let recv_ns = self.clock.now_ns();
        staleness.touch(stream, Instant::now(), recv_ns);

        if self.streams.get(stream).is_none() {
            tracing::warn!(
                stream,
                "a payload arrived on a stream we never subscribed to"
            );
            self.record_payload(recv_ns, stream, payload)?;
            return Ok(Step::Continue);
        }

        let value = match serde_json::from_str(payload.get()) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(stream, %error, "dropping an unparseable payload");
                self.record_payload(recv_ns, stream, payload)?;
                return Ok(Step::Continue);
            }
        };

        let normalized = match normalize(stream, &value, recv_ns) {
            Ok(normalized) => normalized,
            Err(error) => {
                tracing::warn!(stream, %error, "dropping a payload we cannot normalize");
                self.record_payload(recv_ns, stream, payload)?;
                return Ok(Step::Continue);
            }
        };

        // No policy is passed in here. `observe` looks it up from the
        // subscription that declared the stream, so a book ticker cannot be
        // judged under a trade stream's rule even by mistake.
        let gap = match self
            .tracker
            .observe(stream, normalized.seq.id, normalized.seq.policy)
        {
            Ok(gap) => gap,
            Err(error) => {
                tracing::error!(stream, %error, "dropping a message we cannot judge");
                self.record_payload(recv_ns, stream, payload)?;
                return Ok(Step::Continue);
            }
        };

        // Marker first, then the payload: the gap describes the message that
        // follows it, and replay reads the file in this order.
        if let Some(detail) = &gap {
            tracing::warn!(
                stream,
                kind = ?detail.kind,
                policy = ?detail.policy,
                from = detail.from,
                to = detail.to,
                missing = ?detail.missing,
                "sequence gap"
            );
            self.record(Marker::Gap(detail.clone()))?;
        }
        self.record_payload(recv_ns, stream, payload)?;

        if let Some(detail) = gap {
            if self
                .emit(IngestMsg::Gap {
                    stream: detail.stream.clone(),
                    detail: describe_gap(&detail),
                })
                .await
                == Flow::Shutdown
            {
                return Ok(Step::Shutdown);
            }
        }

        Ok(match self.emit(IngestMsg::Market(normalized.event)).await {
            Flow::Continue => Step::Continue,
            Flow::Shutdown => Step::Shutdown,
        })
    }

    /// Bookkeeping for a lost or failed connection.
    async fn disconnected(&mut self, reason: &str) -> Result<Flow, SourceError> {
        tracing::error!(reason, "market stream disconnected");
        self.record(Marker::Disconnect(DisconnectDetail {
            reason: reason.to_owned(),
        }))?;
        // Every stream we have already seen now has an outage in front of it.
        // The first message on each after we reconnect is flagged against the
        // last id from before, under that stream's own policy.
        self.tracker.mark_outage();
        Ok(self
            .emit(IngestMsg::Disconnected {
                reason: reason.to_owned(),
            })
            .await)
    }

    async fn emit(&self, message: IngestMsg) -> Flow {
        // A blocking send, on purpose. If the engine cannot keep up, the socket
        // stops being read and the exchange eventually drops us - which is loud.
        // The alternative is dropping market data on the floor to keep the
        // process looking healthy, and silent loss is exactly what this project
        // refuses.
        match self.tx.send(message).await {
            Ok(()) => Flow::Continue,
            Err(_) => {
                tracing::info!("the engine stopped reading; market source shutting down");
                Flow::Shutdown
            }
        }
    }

    fn record(&mut self, marker: Marker) -> Result<(), SourceError> {
        if let Some(recorder) = &mut self.recorder {
            recorder.record_marker(self.clock.now_ns(), marker)?;
        }
        Ok(())
    }

    fn record_payload(
        &mut self,
        recv_ns: i64,
        stream: &str,
        payload: &RawValue,
    ) -> Result<(), SourceError> {
        if let Some(recorder) = &mut self.recorder {
            recorder.record_payload(recv_ns, stream, payload)?;
        }
        Ok(())
    }
}

// --- internals ---

/// Why a session ended.
#[derive(Debug)]
enum SessionEnd {
    /// The consumer went away. Stop for good.
    Shutdown,
    /// The connection is gone. Reconnect under backoff.
    Ended(String),
}

/// What to do after handling one message.
#[derive(Debug)]
enum Step {
    Continue,
    Shutdown,
    Ended(String),
}

/// Whether the consumer is still there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flow {
    Continue,
    Shutdown,
}

/// What woke the read loop.
#[derive(Debug)]
enum Wakeup {
    Shutdown,
    AckOverdue,
    Staleness,
    Message(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
}

/// Sleep until `deadline`, or never if there is nothing to wait for.
async fn maybe_sleep(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Per-stream silence detection.
///
/// Every subscribed stream is armed the moment the connection comes up, so a
/// stream that never says anything at all is reported - that being the failure
/// mode a "reset on first message" design would miss entirely.
#[derive(Debug)]
struct Staleness {
    bound: Duration,
    /// Stream -> (when we last heard from it, and our ingest time then).
    last: HashMap<String, (Instant, i64)>,
    /// Streams already reported. One report per silent episode: an operator
    /// needs to know a feed went quiet, not to be told so on a timer.
    flagged: HashSet<String>,
}

impl Staleness {
    fn new(bound: Duration, streams: &StreamSet, at: Instant, recv_ns: i64) -> Self {
        Self {
            bound,
            last: streams
                .subscriptions()
                .iter()
                .map(|s| (s.name().to_owned(), (at, recv_ns)))
                .collect(),
            flagged: HashSet::new(),
        }
    }

    fn touch(&mut self, stream: &str, at: Instant, recv_ns: i64) {
        if let Some(slot) = self.last.get_mut(stream) {
            *slot = (at, recv_ns);
            self.flagged.remove(stream);
        }
    }

    /// When the earliest still-healthy stream would go stale.
    fn next_deadline(&self) -> Option<Instant> {
        self.last
            .iter()
            .filter(|(stream, _)| !self.flagged.contains(*stream))
            .map(|(_, (at, _))| *at + self.bound)
            .min()
    }

    /// The streams that have now been silent for too long, marking them so they
    /// are reported once rather than on every tick.
    fn take_due(&mut self, now: Instant) -> Vec<(String, i64)> {
        let mut due: Vec<(String, i64)> = self
            .last
            .iter()
            .filter(|(stream, (at, _))| {
                !self.flagged.contains(*stream) && now.saturating_duration_since(*at) >= self.bound
            })
            .map(|(stream, (_, since_ns))| (stream.clone(), *since_ns))
            .collect();
        // Deterministic order, so a test does not depend on hash iteration.
        due.sort_unstable();
        for (stream, _) in &due {
            self.flagged.insert(stream.clone());
        }
        due
    }
}

/// Render a gap for [`IngestMsg::Gap`].
///
/// Always names the policy and the kind. `domain` cannot hold the structured
/// form without learning Binance's sequencing rules, and a message that said
/// only "100 -> 137" would be read as a countable loss on a stream where the
/// number means nothing of the sort.
fn describe_gap(detail: &GapDetail) -> String {
    let policy = match detail.policy {
        SeqPolicy::Contiguous => "contiguous",
        SeqPolicy::Monotonic => "monotonic",
    };
    let kind = match detail.kind {
        crate::gap::GapKind::Missed => "missed",
        crate::gap::GapKind::Regressed => "regressed",
        crate::gap::GapKind::Outage => "outage",
    };
    match detail.missing {
        Some(missing) => format!(
            "{kind} on a {policy} stream: {} -> {}, {missing} message(s) lost",
            detail.from, detail.to
        ),
        None => format!(
            "{kind} on a {policy} stream: {} -> {}, loss not countable under this policy",
            detail.from, detail.to
        ),
    }
}

/// A connection failure, as a reason string for a `Disconnected` message.
///
/// Rendered with its source chain, because "could not open a WebSocket" on its
/// own tells an operator nothing about whether it was DNS, TLS, or a refusal.
fn describe_connect(error: &ConnectError) -> String {
    let mut text = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use domain::Symbol;

    use super::*;
    use crate::gap::GapKind;
    use crate::normalize::StreamKind;

    fn streams() -> StreamSet {
        StreamSet::new(
            &[Symbol::new("BTCUSDT").expect("valid symbol")],
            &[StreamKind::BookTicker, StreamKind::Trade],
        )
        .expect("a valid stream set")
    }

    #[test]
    fn a_gap_description_always_names_the_policy_and_the_kind() {
        // `domain` gets a string, so the string has to carry what the structured
        // form would have. A bare "100 -> 137" would read as a countable loss on
        // a stream where the id counts something else entirely.
        let countable = describe_gap(&GapDetail {
            stream: "btcusdt@trade".to_owned(),
            policy: SeqPolicy::Contiguous,
            kind: GapKind::Missed,
            from: 100,
            to: 137,
            missing: Some(36),
        });
        assert_eq!(
            countable,
            "missed on a contiguous stream: 100 -> 137, 36 message(s) lost"
        );

        let uncountable = describe_gap(&GapDetail {
            stream: "btcusdt@bookTicker".to_owned(),
            policy: SeqPolicy::Monotonic,
            kind: GapKind::Outage,
            from: 7_000,
            to: 9_000,
            missing: None,
        });
        assert_eq!(
            uncountable,
            "outage on a monotonic stream: 7000 -> 9000, loss not countable under this policy"
        );
    }

    #[test]
    fn every_subscribed_stream_is_armed_for_staleness_from_the_moment_we_connect() {
        // A stream that never produces a single message is exactly the failure a
        // "start the clock on the first message" design cannot see.
        let streams = streams();
        let start = Instant::now();
        let mut staleness = Staleness::new(Duration::from_secs(5), &streams, start, 1_000);

        assert_eq!(staleness.take_due(start), vec![]);
        assert_eq!(
            staleness.take_due(start + Duration::from_secs(5)),
            vec![
                ("btcusdt@bookTicker".to_owned(), 1_000),
                ("btcusdt@trade".to_owned(), 1_000),
            ]
        );
    }

    #[test]
    fn a_stream_is_reported_once_per_silent_episode_and_re_arms_when_it_speaks() {
        let streams = streams();
        let start = Instant::now();
        let mut staleness = Staleness::new(Duration::from_secs(5), &streams, start, 1_000);

        let late = start + Duration::from_secs(6);
        assert_eq!(staleness.take_due(late).len(), 2);
        assert_eq!(
            staleness.take_due(late + Duration::from_secs(60)),
            vec![],
            "silence is reported once, not on a timer"
        );

        let speaks = late + Duration::from_secs(61);
        staleness.touch("btcusdt@trade", speaks, 2_000);
        assert_eq!(
            staleness.take_due(speaks + Duration::from_secs(5)),
            vec![("btcusdt@trade".to_owned(), 2_000)],
            "a recovered stream can go stale again"
        );
    }

    #[test]
    fn only_the_streams_own_messages_keep_it_alive() {
        let streams = streams();
        let start = Instant::now();
        let mut staleness = Staleness::new(Duration::from_secs(5), &streams, start, 1_000);

        // A busy trade stream must not mask a silent book ticker.
        for tick in 1..=10 {
            staleness.touch(
                "btcusdt@trade",
                start + Duration::from_secs(tick),
                1_000 + tick as i64,
            );
        }
        assert_eq!(
            staleness.take_due(start + Duration::from_secs(10)),
            vec![("btcusdt@bookTicker".to_owned(), 1_000)]
        );
    }

    #[test]
    fn the_next_deadline_is_the_earliest_stream_that_could_still_go_stale() {
        let streams = streams();
        let start = Instant::now();
        let bound = Duration::from_secs(5);
        let mut staleness = Staleness::new(bound, &streams, start, 1_000);

        assert_eq!(staleness.next_deadline(), Some(start + bound));

        staleness.touch("btcusdt@trade", start + Duration::from_secs(2), 2_000);
        assert_eq!(
            staleness.next_deadline(),
            Some(start + bound),
            "the book ticker is still the earliest"
        );

        // Once every stream is flagged there is nothing left to wait for.
        staleness.take_due(start + Duration::from_secs(60));
        assert_eq!(staleness.next_deadline(), None);
    }

    #[test]
    fn a_touch_on_an_unsubscribed_stream_creates_no_state() {
        let streams = streams();
        let start = Instant::now();
        let mut staleness = Staleness::new(Duration::from_secs(5), &streams, start, 1_000);
        staleness.touch("ethusdt@trade", start, 9_999);
        assert_eq!(staleness.last.len(), 2);
    }
}
