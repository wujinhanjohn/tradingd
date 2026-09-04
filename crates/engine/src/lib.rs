//! The runtime orchestrator: logging setup, the environment guard, and the run loop.
//!
//! Milestone 2 gives the loop a live market feed. It logs what arrives and does
//! not route it to the strategy - the strategy seam exists and is held, but
//! wiring events into it is milestone 5.
//!
//! Note what this crate does *not* depend on: the `strategy` crate, and the
//! `exchange` crate. The engine only ever sees a [`Box<dyn Strategy>`] and a
//! [`tokio::sync::mpsc::Receiver<IngestMsg>`], so it cannot name a concrete
//! strategy or a concrete market source even by accident. `bot` injects both,
//! and swapping the live feed for a replay source later is a change there and
//! nowhere else.
//!
//! # Fail-closed on a dead feed
//!
//! If the market channel closes, the source that owned the sender is gone. The
//! loop treats that as critical and stops. A process that keeps running,
//! heartbeating, and looking healthy while its market data has silently stopped
//! is precisely the failure this project refuses; with nothing to trade in M2,
//! "halt" means a clean stop.

use std::future::Future;
use std::time::Duration;

use domain::{IngestMsg, MarketEvent, Strategy, Timestamp};
use settings::{Config, Credentials};
use tokio::sync::mpsc::Receiver;

pub mod logging;

mod error;
mod guard;

pub use error::Error;
pub use guard::guard_environment;

/// How often the idle loop emits a heartbeat.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Why the run loop stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    /// A shutdown signal - Ctrl-C in production.
    Signal,
    /// The market source went away, taking its sender with it. Fail-closed.
    FeedClosed,
}

/// What one run did. Returned so tests can assert on behaviour rather than on
/// log output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunSummary {
    pub heartbeats: u64,
    /// Normalized market events received. Not routed anywhere yet.
    pub market_events: u64,
    /// Health messages: gaps, staleness, connects, disconnects.
    pub health_messages: u64,
    pub stopped_by: StopReason,
}

/// Owns the configuration, the credentials, the strategy, and the market feed,
/// and runs the loop.
pub struct Engine {
    config: Config,
    credentials: Credentials,
    /// Deliberately a trait object. See the module docs.
    strategy: Box<dyn Strategy>,
    /// Deliberately a channel, not a trait. The seam between a market source and
    /// the engine is plain data in an ordered stream; an abstraction drawn
    /// around one implementation would be a guess, and the second one (replay)
    /// does not exist yet.
    market_rx: Receiver<IngestMsg>,
}

impl Engine {
    #[must_use]
    pub fn new(
        config: Config,
        credentials: Credentials,
        strategy: Box<dyn Strategy>,
        market_rx: Receiver<IngestMsg>,
    ) -> Self {
        Self {
            config,
            credentials,
            strategy,
            market_rx,
        }
    }

    /// Run until interrupted, or until the market feed dies.
    ///
    /// # Errors
    ///
    /// Currently infallible in practice, but returns [`Error`] so later
    /// milestones can fail without changing every caller. A dead feed is a
    /// *clean* stop here, not an error: the source that died reports its own
    /// reason, and `bot` propagates that rather than inventing a second one.
    pub async fn run(self) -> Result<(), Error> {
        let shutdown = async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                // Fail closed. A process we cannot interrupt is worse than one
                // that stops now, so treat this as a reason to shut down.
                tracing::error!(
                    %error,
                    "cannot listen for Ctrl-C; stopping rather than running uninterruptible"
                );
            }
        };
        self.run_with_shutdown(HEARTBEAT_INTERVAL, shutdown).await?;
        Ok(())
    }

    /// The loop itself, with the heartbeat interval and shutdown trigger injected
    /// so tests can drive it deterministically under paused time.
    async fn run_with_shutdown<S>(
        self,
        heartbeat: Duration,
        shutdown: S,
    ) -> Result<RunSummary, Error>
    where
        S: Future<Output = ()>,
    {
        // Destructured up front so the `select!` arm that borrows the receiver
        // does not collide with the arms that touch the rest of the engine.
        let Self {
            config,
            credentials,
            strategy,
            mut market_rx,
        } = self;

        tracing::info!(
            environment = %config.environment,
            strategy = strategy.name(),
            heartbeat_secs = heartbeat.as_secs(),
            symbols = ?config.market.symbols,
            staleness_ms = config.market.staleness_ms,
            "engine running"
        );
        // Uses the redacting Debug impl: this prints `<redacted>`, never a secret.
        tracing::debug!(credentials = ?credentials, "credentials loaded");

        let mut ticker = tokio::time::interval(heartbeat);
        let mut heartbeats: u64 = 0;
        let mut market_events: u64 = 0;
        let mut health_messages: u64 = 0;
        // The exchange time of the most recent market event. Light bookkeeping,
        // for the heartbeat line only - enough for an operator to see at a
        // glance whether the feed is moving.
        let mut last_event_time: Option<Timestamp> = None;
        tokio::pin!(shutdown);

        let stopped_by = loop {
            tokio::select! {
                () = &mut shutdown => {
                    tracing::info!("shutdown requested");
                    break StopReason::Signal;
                }

                _ = ticker.tick() => {
                    heartbeats += 1;
                    tracing::debug!(
                        beat = heartbeats,
                        market_events,
                        last_event_time,
                        "heartbeat"
                    );
                }

                message = market_rx.recv() => match message {
                    Some(message) => {
                        if let Some(event_time) = log_ingest(&message) {
                            market_events += 1;
                            last_event_time = Some(event_time);
                        } else {
                            health_messages += 1;
                        }
                    }
                    None => {
                        // The sender is gone, which means the source task has
                        // ended - fatally, or because it was told to. Either way
                        // there is no feed, and a running engine with a dead
                        // feed is the silent degradation this project refuses.
                        tracing::error!(
                            market_events,
                            last_event_time,
                            "the market source has stopped and the feed is closed; \
                             shutting down rather than running blind"
                        );
                        break StopReason::FeedClosed;
                    }
                },
            }
        };

        tracing::info!(
            heartbeats,
            market_events,
            health_messages,
            ?stopped_by,
            "engine stopped cleanly"
        );

        Ok(RunSummary {
            heartbeats,
            market_events,
            health_messages,
            stopped_by,
        })
    }
}

/// Log one ingest message, returning the event time if it carried market data.
///
/// Free function rather than a method: it needs nothing from the engine, and
/// keeping it out of `&mut self` is what lets the `select!` arm above borrow the
/// receiver without a fight.
fn log_ingest(message: &IngestMsg) -> Option<Timestamp> {
    match message {
        // Not routed to the strategy - that seam is milestone 5. Logged at
        // debug because a liquid pair produces several of these a second.
        IngestMsg::Market(MarketEvent::BookTicker(book)) => {
            tracing::debug!(
                symbol = %book.symbol,
                bid = %book.bid.0,
                bid_qty = %book.bid_qty.0,
                ask = %book.ask.0,
                ask_qty = %book.ask_qty.0,
                event_time = book.event_time,
                "book ticker"
            );
            Some(book.event_time)
        }

        IngestMsg::Market(MarketEvent::Trade(trade)) => {
            tracing::debug!(
                symbol = %trade.symbol,
                price = %trade.price.0,
                qty = %trade.qty.0,
                event_time = trade.event_time,
                "trade"
            );
            Some(trade.event_time)
        }

        // A gap is missing data, not a lost connection: warn rather than error,
        // so the two are distinguishable at a glance in a log.
        IngestMsg::Gap { stream, detail } => {
            tracing::warn!(stream, detail, "market data gap");
            None
        }

        IngestMsg::Stale { stream, since_ns } => {
            tracing::error!(stream, since_ns, "market stream is stale");
            None
        }

        IngestMsg::Connected => {
            tracing::info!("market feed connected");
            None
        }

        IngestMsg::Disconnected { reason } => {
            tracing::error!(reason, "market feed disconnected");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use domain::{Action, BookTicker, Price, Qty, StrategyCtx, Symbol, Trade};
    use secrecy::SecretString;
    use settings::{
        BinanceConfig, Env, FiltersConfig, LoggingConfig, MarketConfig, RecordingConfig, StreamKind,
    };
    use tokio::sync::mpsc;

    use super::*;

    /// A local stand-in, so the engine's own tests never name a real strategy.
    struct Silent;

    impl Strategy for Silent {
        fn name(&self) -> &str {
            "silent"
        }
        fn on_market(&mut self, _: &MarketEvent, _: &StrategyCtx<'_>) -> Vec<Action> {
            Vec::new()
        }
    }

    fn config() -> Config {
        Config {
            environment: Env::Testnet,
            binance: BinanceConfig {
                spot_rest_url: "https://testnet.binance.vision".to_owned(),
                spot_ws_url: "wss://stream.testnet.binance.vision/stream".to_owned(),
                recv_window_ms: 5000,
            },
            market: MarketConfig {
                symbols: vec!["BTCUSDT".to_owned()],
                streams: vec![StreamKind::BookTicker, StreamKind::Trade],
                staleness_ms: 10_000,
            },
            filters: FiltersConfig {
                refresh_interval_ms: 300_000,
                max_age_ms: 900_000,
            },
            recording: RecordingConfig {
                enabled: false,
                dir: "recordings".into(),
            },
            logging: LoggingConfig {
                level: "info".to_owned(),
                json: false,
            },
        }
    }

    fn credentials() -> Credentials {
        Credentials {
            api_key: SecretString::from("test-key"),
            api_secret: SecretString::from("test-secret"),
        }
    }

    /// An engine plus the sender feeding it. Holding the sender is what keeps the
    /// feed "alive"; dropping it is how a test kills the source.
    fn engine_with_feed() -> (Engine, mpsc::Sender<IngestMsg>) {
        let (tx, rx) = mpsc::channel(64);
        (
            Engine::new(config(), credentials(), Box::new(Silent), rx),
            tx,
        )
    }

    /// An engine whose feed is alive but silent, for the timing tests.
    fn engine() -> (Engine, mpsc::Sender<IngestMsg>) {
        engine_with_feed()
    }

    fn book_ticker(event_time: Timestamp) -> IngestMsg {
        IngestMsg::Market(MarketEvent::BookTicker(BookTicker {
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            bid: Price(domain::Decimal::from(1)),
            bid_qty: Qty(domain::Decimal::from(2)),
            ask: Price(domain::Decimal::from(3)),
            ask_qty: Qty(domain::Decimal::from(4)),
            event_time,
        }))
    }

    fn trade(event_time: Timestamp) -> IngestMsg {
        IngestMsg::Market(MarketEvent::Trade(Trade {
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            price: Price(domain::Decimal::from(5)),
            qty: Qty(domain::Decimal::from(6)),
            event_time,
        }))
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeats_on_the_interval_then_exits_cleanly() {
        // Virtual time: interval fires immediately, then at 10s, 20s, 30s.
        // Shutdown lands at 35s, so four beats.
        let (engine, _feed) = engine();
        let summary = engine
            .run_with_shutdown(
                Duration::from_secs(10),
                tokio::time::sleep(Duration::from_secs(35)),
            )
            .await
            .expect("clean shutdown");
        assert_eq!(summary.heartbeats, 4);
        assert_eq!(summary.stopped_by, StopReason::Signal);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_is_honoured_before_the_first_full_interval() {
        let (engine, _feed) = engine();
        let summary = engine
            .run_with_shutdown(
                Duration::from_secs(10),
                tokio::time::sleep(Duration::from_secs(1)),
            )
            .await
            .expect("clean shutdown");
        // Only the immediate tick at t=0.
        assert_eq!(summary.heartbeats, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_already_complete_shutdown_stops_the_loop_promptly() {
        let (engine, _feed) = engine();
        let summary = engine
            .run_with_shutdown(Duration::from_secs(10), std::future::ready(()))
            .await
            .expect("clean shutdown");
        // select! picks randomly among ready branches, so the immediate tick may
        // or may not land first. What must hold is that the loop stops at once.
        assert!(
            summary.heartbeats <= 1,
            "loop should not keep running: {} heartbeats",
            summary.heartbeats
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_closed_market_channel_stops_the_loop_and_does_not_hang() {
        // The fail-closed seam, at the engine's own level. `bot` proves the same
        // property end to end with a real source; this one pins the loop.
        let (engine, feed) = engine_with_feed();
        drop(feed);

        // No shutdown signal will ever arrive: if the closed channel did not stop
        // the loop, this future would never complete and the test would hang.
        let summary = tokio::time::timeout(
            Duration::from_secs(60),
            engine.run_with_shutdown(Duration::from_secs(10), std::future::pending()),
        )
        .await
        .expect("a dead feed must stop the engine, not hang it")
        .expect("a clean stop");

        assert_eq!(summary.stopped_by, StopReason::FeedClosed);
        assert_eq!(summary.market_events, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn the_feed_is_drained_before_its_closure_stops_the_loop() {
        // Messages already in the channel are not thrown away when the source
        // dies: an mpsc receiver yields the buffered items first and only then
        // reports the close. A source that died right after reporting a gap must
        // not take that gap to the grave with it.
        let (engine, feed) = engine_with_feed();
        feed.send(book_ticker(1_700_000_000_000))
            .await
            .expect("open channel");
        feed.send(IngestMsg::Gap {
            stream: "btcusdt@trade".to_owned(),
            detail: "missed on a contiguous stream: 100 -> 137, 36 message(s) lost".to_owned(),
        })
        .await
        .expect("open channel");
        feed.send(trade(1_700_000_000_500))
            .await
            .expect("open channel");
        drop(feed);

        let summary = tokio::time::timeout(
            Duration::from_secs(60),
            engine.run_with_shutdown(Duration::from_secs(10), std::future::pending()),
        )
        .await
        .expect("must not hang")
        .expect("a clean stop");

        assert_eq!(summary.stopped_by, StopReason::FeedClosed);
        assert_eq!(summary.market_events, 2);
        assert_eq!(summary.health_messages, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn every_ingest_message_is_accounted_for_as_data_or_health() {
        let (engine, feed) = engine_with_feed();
        for message in [
            IngestMsg::Connected,
            book_ticker(1_700_000_000_000),
            trade(1_700_000_000_100),
            IngestMsg::Gap {
                stream: "btcusdt@trade".to_owned(),
                detail: "outage on a contiguous stream: 1 -> 5, 3 message(s) lost".to_owned(),
            },
            IngestMsg::Stale {
                stream: "btcusdt@bookTicker".to_owned(),
                since_ns: 1_700_000_000_000_000_000,
            },
            IngestMsg::Disconnected {
                reason: "socket error: connection reset".to_owned(),
            },
        ] {
            feed.send(message).await.expect("open channel");
        }
        drop(feed);

        let summary = tokio::time::timeout(
            Duration::from_secs(60),
            engine.run_with_shutdown(Duration::from_secs(10), std::future::pending()),
        )
        .await
        .expect("must not hang")
        .expect("a clean stop");

        assert_eq!(summary.market_events, 2);
        assert_eq!(summary.health_messages, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn the_loop_never_asks_the_strategy_to_do_anything_in_m2() {
        // The seam exists and the engine now holds a live feed, but events are
        // still not routed. If milestone 5 starts feeding the strategy, this test
        // should be replaced deliberately, not deleted.
        let (engine, feed) = engine_with_feed();
        feed.send(book_ticker(1_700_000_000_000))
            .await
            .expect("open channel");

        let summary = engine
            .run_with_shutdown(
                Duration::from_secs(5),
                tokio::time::sleep(Duration::from_secs(12)),
            )
            .await
            .expect("clean shutdown");
        assert_eq!(summary.heartbeats, 3);
        assert_eq!(summary.market_events, 1, "received, and only logged");
    }

    #[test]
    fn engine_accepts_any_boxed_strategy_and_any_market_source() {
        // The compile-time half of the guarantee: `new` takes a trait object and
        // a plain channel, so neither a different strategy nor a different market
        // source (a replay, later) needs an engine change.
        let boxed: Box<dyn Strategy> = Box::new(Silent);
        let (_tx, rx) = mpsc::channel(1);
        let _ = Engine::new(config(), credentials(), boxed, rx);
    }
}
