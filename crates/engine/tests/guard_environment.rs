//! Exercises `guard_environment` against the real process environment, which is
//! the path `bot` actually takes. The pure decision table is unit-tested inside
//! the crate; this checks the wiring to the environment is right.

use std::sync::{Mutex, MutexGuard, OnceLock};

use engine::Error;
use settings::{
    BinanceConfig, Config, Env, FiltersConfig, LoggingConfig, MarketConfig, RecordingConfig,
    StreamKind, PRODUCTION_CONFIRMATION_VALUE, PRODUCTION_CONFIRMATION_VAR,
};

fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Restores the variable when dropped, so a failed assertion cannot leak state.
struct ScopedVar {
    previous: Option<String>,
}

impl ScopedVar {
    fn set(value: Option<&str>) -> Self {
        let previous = std::env::var(PRODUCTION_CONFIRMATION_VAR).ok();
        match value {
            Some(v) => std::env::set_var(PRODUCTION_CONFIRMATION_VAR, v),
            None => std::env::remove_var(PRODUCTION_CONFIRMATION_VAR),
        }
        Self { previous }
    }
}

impl Drop for ScopedVar {
    fn drop(&mut self) {
        match &self.previous {
            Some(v) => std::env::set_var(PRODUCTION_CONFIRMATION_VAR, v),
            None => std::env::remove_var(PRODUCTION_CONFIRMATION_VAR),
        }
    }
}

fn config(environment: Env) -> Config {
    Config {
        environment,
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

#[test]
fn testnet_starts_with_nothing_set() {
    let _guard = env_lock();
    let _var = ScopedVar::set(None);
    assert!(engine::guard_environment(&config(Env::Testnet)).is_ok());
}

#[test]
fn production_refuses_to_start_when_the_variable_is_absent() {
    let _guard = env_lock();
    let _var = ScopedVar::set(None);

    let err = engine::guard_environment(&config(Env::Production)).expect_err("must refuse");
    assert!(
        matches!(err, Error::ProductionNotConfirmed { .. }),
        "{err:?}"
    );
    let msg = err.to_string();
    assert!(msg.contains(PRODUCTION_CONFIRMATION_VAR), "{msg}");
    assert!(msg.contains(PRODUCTION_CONFIRMATION_VALUE), "{msg}");
}

#[test]
fn production_refuses_to_start_for_a_merely_truthy_value() {
    let _guard = env_lock();
    for sloppy in ["1", "true", "yes", "i_understand_the_risk", ""] {
        let _var = ScopedVar::set(Some(sloppy));
        let result = engine::guard_environment(&config(Env::Production));
        assert!(
            matches!(result, Err(Error::ProductionNotConfirmed { .. })),
            "`{sloppy}` must not arm production, got {result:?}"
        );
    }
}

#[test]
fn production_starts_only_with_the_exact_confirmation() {
    let _guard = env_lock();
    let _var = ScopedVar::set(Some(PRODUCTION_CONFIRMATION_VALUE));
    assert!(engine::guard_environment(&config(Env::Production)).is_ok());
}

#[test]
fn a_stray_confirmation_does_not_change_testnet() {
    let _guard = env_lock();
    let _var = ScopedVar::set(Some(PRODUCTION_CONFIRMATION_VALUE));
    assert!(engine::guard_environment(&config(Env::Testnet)).is_ok());
}
