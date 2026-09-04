//! Configuration and credential loading.
//!
//! Two deliberately separate things live here:
//!
//! - [`Config`] comes from a TOML file plus `APP_`-prefixed environment
//!   overrides. It holds nothing secret, so it is safe to log in full.
//! - [`Credentials`] come from the process environment only, are wrapped in
//!   [`secrecy::SecretString`], and are never logged, `Debug`-printed, or
//!   serialized.
//!
//! Loading fails closed: a missing file, an unknown key, an out-of-range value,
//! or an absent credential is a typed [`Error`] and a refusal to start, never a
//! guess at what was meant.

mod config;
mod credentials;
mod error;

pub use config::{
    load, BinanceConfig, Config, Env, FiltersConfig, LoggingConfig, MarketConfig, RecordingConfig,
    StreamKind, ENV_NESTED_SEPARATOR, ENV_PREFIX,
};
pub use credentials::{
    load_credentials, production_confirmed, Credentials, API_KEY_VAR, API_SECRET_VAR,
    PRODUCTION_CONFIRMATION_VALUE, PRODUCTION_CONFIRMATION_VAR,
};
pub use error::Error;
