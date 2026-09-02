use std::fmt;
use std::path::{Path, PathBuf};

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

/// The narrowest staleness bound we will accept.
///
/// Below this, ordinary scheduling jitter on a loaded machine reads as a dead
/// feed, and an alert that cries wolf is worse than no alert at all.
const MIN_STALENESS_MS: u64 = 100;

/// The widest staleness bound we will accept.
///
/// A liquid pair like BTCUSDT ticks several times a second, so five minutes of
/// silence is already far past anything that could be called healthy. A bound
/// looser than this is not a bound.
const MAX_STALENESS_MS: u64 = 300_000;

/// Loopback authorities, for which a plaintext `ws://` market URL is accepted.
///
/// This exists so the end-to-end tests can point the *real binary* at a local
/// fake WebSocket server instead of at the live exchange. It is not a security
/// boundary and is not relied on as one: `exchange::require_class` is the gate,
/// and it re-parses the URL with a deliberately pedantic authority parser that
/// refuses userinfo, percent-encoding, backslashes, and anything else that could
/// make two parsers disagree about where the host ends. The check here only
/// turns a plainly wrong URL into a readable error at load time.
const LOOPBACK_PREFIXES: &[&str] = &["ws://127.0.0.1:", "ws://localhost:", "ws://[::1]:"];

/// Non-secret runtime configuration. Safe to log in full - by construction there
/// is nowhere in here for a credential to hide.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub environment: Env,
    pub binance: BinanceConfig,
    pub market: MarketConfig,
    pub recording: RecordingConfig,
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

/// What market data to subscribe to, and when silence counts as a fault.
///
/// `symbols` are held as strings here and validated through
/// [`domain::Symbol::new`], because `domain` carries no `serde` and is not going
/// to start: it depends on `rust_decimal` and `thiserror` and nothing else. The
/// validation still happens at load time, so a bad symbol refuses to start
/// rather than surfacing at the first subscribe.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketConfig {
    /// Trading pairs, uppercase, e.g. `["BTCUSDT"]`. Non-empty, no duplicates.
    pub symbols: Vec<String>,
    /// Which streams to subscribe to for each symbol. Non-empty, no duplicates.
    pub streams: Vec<StreamKind>,
    /// Silence on a subscribed stream longer than this is reported as stale.
    pub staleness_ms: u64,
}

/// A market stream this build can subscribe to.
///
/// Spelled in our own snake_case rather than Binance's `bookTicker`, because
/// this is our configuration namespace and the exchange's wire spelling is the
/// adapter's business. `bot` maps this to `exchange::StreamKind` in one `match`,
/// the same way it maps [`Env`] to `exchange::EndpointClass` - which is what
/// keeps `settings` from depending on `exchange`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StreamKind {
    /// Best bid and ask, pushed on every book change.
    BookTicker,
    /// Individual trades.
    Trade,
}

/// Whether to write a session recording, and where.
///
/// Enabling this is fail-closed downstream: if the directory cannot be prepared,
/// the market source refuses to start rather than running unrecorded. Believing
/// a session was captured when it was not is the one failure that leaves nothing
/// behind to notice it by.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingConfig {
    pub enabled: bool,
    /// Directory for session files. Created if absent.
    pub dir: PathBuf,
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

impl StreamKind {
    /// The name used in configuration and in logs.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BookTicker => "book_ticker",
            Self::Trade => "trade",
        }
    }
}

impl fmt::Display for StreamKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl MarketConfig {
    /// The configured symbols, as validated domain values.
    ///
    /// Fallible rather than unwrapping behind a "validation already proved this"
    /// comment: a `Config` built by hand in a test has not been through
    /// [`load`], and a latent panic on that path is worth avoiding for the cost
    /// of one `?` at the single call site in `bot`.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidValue`] naming the first symbol `domain` will not vouch
    /// for. After a successful [`load`] this cannot fail.
    pub fn symbols(&self) -> Result<Vec<domain::Symbol>, Error> {
        self.symbols
            .iter()
            .map(|raw| {
                domain::Symbol::new(raw).map_err(|source| Error::InvalidValue {
                    field: "market.symbols",
                    reason: format!(
                        "{source}. Symbols are uppercase ASCII alphanumeric, e.g. \"BTCUSDT\""
                    ),
                })
            })
            .collect()
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
        require_ws_scheme("binance.spot_ws_url", &self.binance.spot_ws_url)?;

        if self.binance.recv_window_ms == 0 || self.binance.recv_window_ms > MAX_RECV_WINDOW_MS {
            return Err(Error::InvalidValue {
                field: "binance.recv_window_ms",
                reason: format!(
                    "must be between 1 and {MAX_RECV_WINDOW_MS}, got {}",
                    self.binance.recv_window_ms
                ),
            });
        }

        // Proves every symbol parses, at load time rather than at first subscribe.
        let _ = self.market.symbols()?;
        require_non_empty_unique("market.symbols", &self.market.symbols)?;
        require_non_empty_unique("market.streams", &self.market.streams)?;

        if !(MIN_STALENESS_MS..=MAX_STALENESS_MS).contains(&self.market.staleness_ms) {
            return Err(Error::InvalidValue {
                field: "market.staleness_ms",
                reason: format!(
                    "must be between {MIN_STALENESS_MS} and {MAX_STALENESS_MS}, got {}",
                    self.market.staleness_ms
                ),
            });
        }

        if self.recording.enabled && self.recording.dir.as_os_str().is_empty() {
            return Err(Error::InvalidValue {
                field: "recording.dir",
                reason: "must name a directory when recording is enabled".to_owned(),
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

/// Reject an empty or duplicate-bearing list.
///
/// A duplicate is refused rather than deduplicated: a stream subscribed twice
/// would be tracked once and counted twice, and silently "fixing" the config
/// leaves the operator believing they configured something they did not.
fn require_non_empty_unique<T>(field: &'static str, values: &[T]) -> Result<(), Error>
where
    T: PartialEq + fmt::Debug,
{
    if values.is_empty() {
        return Err(Error::InvalidValue {
            field,
            reason: "must not be empty".to_owned(),
        });
    }

    for (index, value) in values.iter().enumerate() {
        if values[..index].contains(value) {
            return Err(Error::InvalidValue {
                field,
                reason: format!("{value:?} appears more than once"),
            });
        }
    }

    Ok(())
}

/// The market WebSocket URL must be `wss://`, or a plaintext loopback address.
///
/// See [`LOOPBACK_PREFIXES`] for why that second case exists and why it is not
/// load-bearing for safety.
fn require_ws_scheme(field: &'static str, value: &str) -> Result<(), Error> {
    if value.starts_with("wss://") || LOOPBACK_PREFIXES.iter().any(|p| value.starts_with(p)) {
        return Ok(());
    }
    Err(Error::InvalidValue {
        field,
        reason: format!(
            "must start with `wss://` (or `ws://` on a loopback address, for local \
             end-to-end tests), got `{value}`"
        ),
    })
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
