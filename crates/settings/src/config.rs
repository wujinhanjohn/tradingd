use std::fmt;
use std::path::Path;

use figment::{
    providers::{Env as EnvProvider, Format, Toml},
    Figment,
};
use serde::Deserialize;

use crate::error::Error;

/// Prefix for environment variables that override config fields.
pub const ENV_PREFIX: &str = "APP_";

/// Separator for nested keys in environment overrides,
/// e.g. `APP_LOGGING__LEVEL=debug`.
pub const ENV_NESTED_SEPARATOR: &str = "__";

/// Binance's hard ceiling for `recvWindow`. A larger value is silently rejected
/// by the exchange, so we refuse it here instead of at first request.
const MAX_RECV_WINDOW_MS: u64 = 60_000;

/// Non-secret runtime configuration. Safe to log in full - by construction there
/// is nowhere in here for a credential to hide.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub environment: Env,
    pub binance: BinanceConfig,
    pub logging: LoggingConfig,
}

/// Which exchange environment to trade against.
///
/// There is no `Default`, on purpose: the field is required, so no config can
/// end up in an environment nobody chose.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum Env {
    Testnet,
    Production,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceConfig {
    pub spot_rest_url: String,
    pub spot_ws_url: String,
    pub recv_window_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// A `tracing-subscriber` filter directive, e.g. `"info"` or `"info,engine=debug"`.
    pub level: String,
    pub json: bool,
}

impl Env {
    #[must_use]
    pub fn is_production(self) -> bool {
        matches!(self, Self::Production)
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Testnet => "testnet",
            Self::Production => "production",
        }
    }
}

impl fmt::Display for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Load configuration from `path`, then apply `APP_`-prefixed environment overrides.
///
/// # Errors
///
/// Returns [`Error`] if the file is missing or unreadable, if a required field is
/// absent, if a field has the wrong type, if an unknown key is present, or if a
/// value is outside its valid range. Never panics on bad input.
pub fn load(path: impl AsRef<Path>) -> Result<Config, Error> {
    let path = path.as_ref();

    // Check existence ourselves so a missing file reports the path the user
    // actually typed, rather than surfacing as a pile of "missing field" errors.
    if !path.exists() {
        return Err(Error::ConfigFileNotFound {
            path: path.to_path_buf(),
        });
    }
    if let Err(source) = std::fs::File::open(path) {
        return Err(Error::ConfigFileUnreadable {
            path: path.to_path_buf(),
            source,
        });
    }

    let config: Config = Figment::new()
        // `file_exact` does not walk up parent directories. Silently loading a
        // config from somewhere the operator did not name is exactly the kind of
        // surprise that gets orders sent to the wrong place.
        .merge(Toml::file_exact(path))
        .merge(EnvProvider::prefixed(ENV_PREFIX).split(ENV_NESTED_SEPARATOR))
        .extract()
        .map_err(|source| Error::InvalidConfig {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;

    config.validate()?;
    Ok(config)
}

impl Config {
    /// Range and shape checks that the type system cannot express.
    fn validate(&self) -> Result<(), Error> {
        require_scheme(
            "binance.spot_rest_url",
            &self.binance.spot_rest_url,
            "https://",
        )?;
        require_scheme("binance.spot_ws_url", &self.binance.spot_ws_url, "wss://")?;

        if self.binance.recv_window_ms == 0 || self.binance.recv_window_ms > MAX_RECV_WINDOW_MS {
            return Err(Error::InvalidValue {
                field: "binance.recv_window_ms",
                reason: format!(
                    "must be between 1 and {MAX_RECV_WINDOW_MS}, got {}",
                    self.binance.recv_window_ms
                ),
            });
        }

        if self.logging.level.trim().is_empty() {
            return Err(Error::InvalidValue {
                field: "logging.level",
                reason: "must not be empty, e.g. \"info\"".to_owned(),
            });
        }

        Ok(())
    }
}

fn require_scheme(field: &'static str, value: &str, scheme: &str) -> Result<(), Error> {
    if value.starts_with(scheme) {
        Ok(())
    } else {
        Err(Error::InvalidValue {
            field,
            reason: format!("must start with `{scheme}`, got `{value}`"),
        })
    }
}
