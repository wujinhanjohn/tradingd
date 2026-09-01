/// Failures that stop the engine from starting or running.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(
        "refusing to start: environment is `production` but `{var}` is not set to \
         `{value}`. Running against production risks real funds; set that variable \
         exactly, or switch the config to `environment = \"testnet\"`"
    )]
    ProductionNotConfirmed {
        var: &'static str,
        value: &'static str,
    },

    #[error("invalid log filter `{directive}`")]
    InvalidLogFilter {
        directive: String,
        #[source]
        source: tracing_subscriber::filter::ParseError,
    },

    #[error("could not install the tracing subscriber (is logging already initialised?)")]
    LoggingInit(#[source] Box<dyn std::error::Error + Send + Sync>),
}
