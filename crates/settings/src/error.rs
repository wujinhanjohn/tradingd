use std::path::PathBuf;

/// Everything that can go wrong loading configuration or credentials.
///
/// Every variant is a refusal to start, not a warning. Configuration we cannot
/// fully understand is treated as a reason to do nothing.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("config file not found: {}", .path.display())]
    ConfigFileNotFound { path: PathBuf },

    #[error("config file {} could not be read", .path.display())]
    ConfigFileUnreadable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid configuration in {}", .path.display())]
    InvalidConfig {
        path: PathBuf,
        // Boxed: `figment::Error` is ~230 bytes and would otherwise bloat every
        // `Result<_, Error>` in the crate (clippy::result_large_err).
        #[source]
        source: Box<figment::Error>,
    },

    #[error("invalid value for `{field}`: {reason}")]
    InvalidValue { field: &'static str, reason: String },

    #[error(
        "missing required environment variable `{name}`. \
         Set it before starting the bot; see .env.example"
    )]
    MissingEnvVar { name: &'static str },

    #[error(
        "environment variable `{name}` is set but empty. \
         An empty credential is never valid; refusing to start"
    )]
    EmptyEnvVar { name: &'static str },
}
