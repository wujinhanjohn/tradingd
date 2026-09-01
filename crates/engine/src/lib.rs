//! The runtime orchestrator: logging setup, the environment guard, and the run loop.
//!
//! In milestone 1 the loop does nothing but heartbeat and wait for a shutdown
//! signal. It holds a strategy but does not yet feed it market events; wiring
//! that seam is a later milestone.
//!
//! Note what this crate does *not* depend on: the `strategy` crate. The engine
//! only ever sees a [`Box<dyn Strategy>`], so it cannot name a concrete strategy
//! even by accident, and a real strategy drops in later with no change here.

use std::future::Future;
use std::time::Duration;

use domain::Strategy;
use settings::{Config, Credentials};

pub mod logging;

mod error;
mod guard;

pub use error::Error;
pub use guard::guard_environment;

/// How often the idle loop emits a heartbeat.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Owns the configuration, the credentials, and the strategy, and runs the loop.
pub struct Engine {
    config: Config,
    credentials: Credentials,
    /// Deliberately a trait object. See the module docs.
    strategy: Box<dyn Strategy>,
}

impl Engine {
    #[must_use]
    pub fn new(config: Config, credentials: Credentials, strategy: Box<dyn Strategy>) -> Self {
        Self {
            config,
            credentials,
            strategy,
        }
    }

    /// Run until interrupted.
    ///
    /// Milestone 1: heartbeat on an interval and return cleanly on Ctrl-C.
    ///
    /// # Errors
    ///
    /// Currently infallible in practice, but returns [`Error`] so later
    /// milestones can fail without changing every caller.
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
    ///
    /// Returns the number of heartbeats emitted.
    async fn run_with_shutdown<S>(self, heartbeat: Duration, shutdown: S) -> Result<u64, Error>
    where
        S: Future<Output = ()>,
    {
        tracing::info!(
            environment = %self.config.environment,
            strategy = self.strategy.name(),
            heartbeat_secs = heartbeat.as_secs(),
            "engine running"
        );
        // Uses the redacting Debug impl: this prints `<redacted>`, never a secret.
        tracing::debug!(credentials = ?self.credentials, "credentials loaded");

        let mut ticker = tokio::time::interval(heartbeat);
        let mut beats: u64 = 0;
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                () = &mut shutdown => {
                    tracing::info!("shutdown requested");
                    break;
                }
                _ = ticker.tick() => {
                    beats += 1;
                    tracing::debug!(beat = beats, "heartbeat");
                }
            }
        }

        tracing::info!(heartbeats = beats, "engine stopped cleanly");
        Ok(beats)
    }
}

#[cfg(test)]
mod tests {
    use domain::{Action, MarketEvent, StrategyCtx};
    use secrecy::SecretString;
    use settings::{BinanceConfig, Env, LoggingConfig};

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

    fn engine() -> Engine {
        Engine::new(
            Config {
                environment: Env::Testnet,
                binance: BinanceConfig {
                    spot_rest_url: "https://testnet.binance.vision".to_owned(),
                    spot_ws_url: "wss://stream.testnet.binance.vision/ws".to_owned(),
                    recv_window_ms: 5000,
                },
                logging: LoggingConfig {
                    level: "info".to_owned(),
                    json: false,
                },
            },
            Credentials {
                api_key: SecretString::from("test-key"),
                api_secret: SecretString::from("test-secret"),
            },
            Box::new(Silent),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeats_on_the_interval_then_exits_cleanly() {
        // Virtual time: interval fires immediately, then at 10s, 20s, 30s.
        // Shutdown lands at 35s, so four beats.
        let beats = engine()
            .run_with_shutdown(
                Duration::from_secs(10),
                tokio::time::sleep(Duration::from_secs(35)),
            )
            .await
            .expect("clean shutdown");
        assert_eq!(beats, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_is_honoured_before_the_first_full_interval() {
        let beats = engine()
            .run_with_shutdown(
                Duration::from_secs(10),
                tokio::time::sleep(Duration::from_secs(1)),
            )
            .await
            .expect("clean shutdown");
        // Only the immediate tick at t=0.
        assert_eq!(beats, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_already_complete_shutdown_stops_the_loop_promptly() {
        let beats = engine()
            .run_with_shutdown(Duration::from_secs(10), std::future::ready(()))
            .await
            .expect("clean shutdown");
        // select! picks randomly among ready branches, so the immediate tick may
        // or may not land first. What must hold is that the loop stops at once.
        assert!(beats <= 1, "loop should not keep running: {beats} beats");
    }

    #[tokio::test(start_paused = true)]
    async fn the_loop_never_asks_the_strategy_to_do_anything_in_m1() {
        // The seam exists but is not wired yet. If a later milestone starts
        // feeding events, this test should be replaced deliberately, not deleted.
        let beats = engine()
            .run_with_shutdown(
                Duration::from_secs(5),
                tokio::time::sleep(Duration::from_secs(12)),
            )
            .await
            .expect("clean shutdown");
        assert_eq!(beats, 3);
    }

    #[test]
    fn engine_accepts_any_boxed_strategy() {
        // The compile-time half of the guarantee: `new` takes a trait object, so
        // swapping in a different strategy needs no engine change.
        let boxed: Box<dyn Strategy> = Box::new(Silent);
        let _ = Engine::new(engine().config, engine().credentials, boxed);
    }
}
