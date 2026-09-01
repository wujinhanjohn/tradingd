//! Structured logging setup.

use settings::LoggingConfig;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

/// Environment variable that overrides `logging.level` from the config file.
pub const FILTER_ENV_VAR: &str = "RUST_LOG";

/// Install the global tracing subscriber.
///
/// `RUST_LOG` takes precedence over `logging.level` when it is set and non-empty.
/// A `RUST_LOG` that cannot be parsed is an error rather than a silent fall back
/// to the config value: an operator who asked for specific logging and quietly
/// did not get it is worse off than one who is told.
///
/// # Errors
///
/// Returns [`Error::InvalidLogFilter`] for an unparseable filter directive, and
/// [`Error::LoggingInit`] if a global subscriber is already installed.
///
/// [`Error::InvalidLogFilter`]: crate::Error::InvalidLogFilter
/// [`Error::LoggingInit`]: crate::Error::LoggingInit
pub fn init(config: &LoggingConfig) -> Result<(), crate::Error> {
    let filter = build_filter(config, |name| std::env::var(name).ok())?;
    let registry = tracing_subscriber::registry().with(filter);

    let installed = if config.json {
        registry
            .with(fmt::layer().json().with_current_span(true))
            .try_init()
    } else {
        registry.with(fmt::layer().with_target(true)).try_init()
    };

    installed.map_err(|source| crate::Error::LoggingInit(Box::new(source)))
}

/// Build the filter, with the environment injected so it can be tested without
/// mutating process-global state or installing a global subscriber.
fn build_filter(
    config: &LoggingConfig,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<EnvFilter, crate::Error> {
    let directive = match lookup(FILTER_ENV_VAR) {
        Some(from_env) if !from_env.trim().is_empty() => from_env,
        _ => config.level.clone(),
    };

    EnvFilter::try_new(&directive)
        .map_err(|source| crate::Error::InvalidLogFilter { directive, source })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(level: &str) -> LoggingConfig {
        LoggingConfig {
            level: level.to_owned(),
            json: false,
        }
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn env_with(value: &'static str) -> impl Fn(&str) -> Option<String> {
        move |name| (name == FILTER_ENV_VAR).then(|| value.to_owned())
    }

    #[test]
    fn uses_the_configured_level_when_the_environment_is_silent() {
        let filter = build_filter(&config("info"), no_env).expect("valid directive");
        assert_eq!(filter.to_string(), "info");
    }

    #[test]
    fn accepts_per_target_directives_from_the_config() {
        let filter = build_filter(&config("info,engine=debug"), no_env).expect("valid");
        assert!(filter.to_string().contains("engine=debug"), "{filter}");
    }

    #[test]
    fn rust_log_overrides_the_configured_level() {
        let filter = build_filter(&config("info"), env_with("debug")).expect("valid");
        assert_eq!(filter.to_string(), "debug");
    }

    #[test]
    fn a_blank_rust_log_falls_back_to_the_config() {
        for blank in ["", "   "] {
            let lookup = |name: &str| (name == FILTER_ENV_VAR).then(|| blank.to_owned());
            let filter = build_filter(&config("warn"), lookup).expect("valid");
            assert_eq!(filter.to_string(), "warn");
        }
    }

    #[test]
    fn an_unparseable_rust_log_is_an_error_not_a_silent_fallback() {
        let err = build_filter(&config("info"), env_with("=====")).expect_err("garbage filter");
        assert!(
            matches!(&err, crate::Error::InvalidLogFilter { directive, .. } if directive == "====="),
            "got {err:?}"
        );
    }

    #[test]
    fn an_unparseable_configured_level_is_an_error_not_a_panic() {
        let err = build_filter(&config("====="), no_env).expect_err("garbage level");
        assert!(
            matches!(err, crate::Error::InvalidLogFilter { .. }),
            "got {err:?}"
        );
    }
}
