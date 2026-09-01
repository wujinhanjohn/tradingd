//! Tests that read and mutate the real process environment.
//!
//! The environment is process-global, so every test here takes `ENV_LOCK` first.
//! They live in their own integration-test binary, which cargo runs as a separate
//! process, so they cannot disturb the tests in `load_config.rs`.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use secrecy::ExposeSecret;
use settings::{
    Env, Error, API_KEY_VAR, API_SECRET_VAR, PRODUCTION_CONFIRMATION_VALUE,
    PRODUCTION_CONFIRMATION_VAR,
};

fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Restores every variable it touched when dropped, so a failing assertion
/// cannot leak state into the next test.
struct ScopedEnv {
    saved: Vec<(&'static str, Option<String>)>,
}

impl ScopedEnv {
    fn new(vars: &[(&'static str, Option<&str>)]) -> Self {
        let saved = vars
            .iter()
            .map(|(name, _)| (*name, std::env::var(name).ok()))
            .collect();
        for (name, value) in vars {
            match value {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
        }
        Self { saved }
    }
}

impl Drop for ScopedEnv {
    fn drop(&mut self) {
        for (name, value) in &self.saved {
            match value {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
        }
    }
}

fn repo_config() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("config.toml")
}

#[test]
fn load_credentials_reads_the_real_environment() {
    let _guard = env_lock();
    let _env = ScopedEnv::new(&[
        (API_KEY_VAR, Some("real-key")),
        (API_SECRET_VAR, Some("real-secret")),
    ]);

    let creds = settings::load_credentials().expect("both vars are set");
    assert_eq!(creds.api_key.expose_secret(), "real-key");
    assert_eq!(creds.api_secret.expose_secret(), "real-secret");
}

#[test]
fn load_credentials_refuses_to_start_without_the_key() {
    let _guard = env_lock();
    let _env = ScopedEnv::new(&[(API_KEY_VAR, None), (API_SECRET_VAR, Some("s"))]);

    let err = settings::load_credentials().expect_err("key is unset");
    assert!(
        matches!(err, Error::MissingEnvVar { name } if name == API_KEY_VAR),
        "{err:?}"
    );
    assert!(err.to_string().contains(API_KEY_VAR), "{err}");
}

#[test]
fn load_credentials_refuses_to_start_without_the_secret() {
    let _guard = env_lock();
    let _env = ScopedEnv::new(&[(API_KEY_VAR, Some("k")), (API_SECRET_VAR, None)]);

    let err = settings::load_credentials().expect_err("secret is unset");
    assert!(
        matches!(err, Error::MissingEnvVar { name } if name == API_SECRET_VAR),
        "{err:?}"
    );
}

#[test]
fn production_confirmation_reads_the_real_environment() {
    let _guard = env_lock();

    {
        let _env = ScopedEnv::new(&[(PRODUCTION_CONFIRMATION_VAR, None)]);
        assert!(
            !settings::production_confirmed(),
            "unset must not arm production"
        );
    }
    {
        let _env = ScopedEnv::new(&[(PRODUCTION_CONFIRMATION_VAR, Some("true"))]);
        assert!(
            !settings::production_confirmed(),
            "`true` must not arm production"
        );
    }
    {
        let _env = ScopedEnv::new(&[(
            PRODUCTION_CONFIRMATION_VAR,
            Some(PRODUCTION_CONFIRMATION_VALUE),
        )]);
        assert!(
            settings::production_confirmed(),
            "exact value must arm production"
        );
    }
}

#[test]
fn app_prefixed_variables_override_config_fields() {
    let _guard = env_lock();
    let _env = ScopedEnv::new(&[
        ("APP_LOGGING__LEVEL", Some("debug")),
        ("APP_LOGGING__JSON", Some("true")),
        ("APP_BINANCE__RECV_WINDOW_MS", Some("1234")),
    ]);

    let config = settings::load(repo_config()).expect("overrides apply cleanly");
    assert_eq!(config.logging.level, "debug");
    assert!(config.logging.json);
    assert_eq!(config.binance.recv_window_ms, 1234);
    // Untouched fields still come from the file.
    assert_eq!(config.environment, Env::Testnet);
}

#[test]
fn an_app_prefixed_typo_is_rejected_rather_than_ignored() {
    let _guard = env_lock();
    let _env = ScopedEnv::new(&[("APP_LOGGING__LEVLE", Some("debug"))]);

    let err = settings::load(repo_config()).expect_err("APP_ typo must be loud");
    assert!(matches!(err, Error::InvalidConfig { .. }), "{err:?}");
}

#[test]
fn credentials_in_the_environment_cannot_reach_the_config_summary() {
    let _guard = env_lock();
    let _env = ScopedEnv::new(&[
        (API_KEY_VAR, Some("LEAKY_KEY_VALUE")),
        (API_SECRET_VAR, Some("LEAKY_SECRET_VALUE")),
    ]);

    // Config is what gets logged at startup. Credentials are not APP_-prefixed
    // and are not Config fields, so there is no path for them to land in here.
    let config = settings::load(repo_config()).expect("valid");
    let rendered = format!("{config:?}");
    assert!(
        !rendered.contains("LEAKY_KEY_VALUE"),
        "api key reached Config: {rendered}"
    );
    assert!(
        !rendered.contains("LEAKY_SECRET_VALUE"),
        "api secret reached Config: {rendered}"
    );
}

#[test]
fn credentials_debug_stays_redacted_when_loaded_from_the_real_environment() {
    let _guard = env_lock();
    let _env = ScopedEnv::new(&[
        (API_KEY_VAR, Some("LEAKY_KEY_VALUE")),
        (API_SECRET_VAR, Some("LEAKY_SECRET_VALUE")),
    ]);

    let creds = settings::load_credentials().expect("valid");
    let rendered = format!("{creds:?}");
    assert!(!rendered.contains("LEAKY_KEY_VALUE"), "{rendered}");
    assert!(!rendered.contains("LEAKY_SECRET_VALUE"), "{rendered}");
    assert!(rendered.contains("<redacted>"), "{rendered}");
}
