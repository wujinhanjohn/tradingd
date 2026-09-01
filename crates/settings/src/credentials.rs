use std::fmt;

use secrecy::SecretString;

use crate::error::Error;

/// Environment variable holding the Binance API key.
pub const API_KEY_VAR: &str = "BINANCE_API_KEY";

/// Environment variable holding the Binance API secret.
pub const API_SECRET_VAR: &str = "BINANCE_API_SECRET";

/// Environment variable that must be set to run against production.
///
/// Deliberately *not* `APP_`-prefixed. `APP_*` is reserved for [`crate::Config`]
/// fields, and arming real-money trading must never be something a checked-in
/// config file can do.
pub const PRODUCTION_CONFIRMATION_VAR: &str = "ALLOW_PRODUCTION";

/// The one value [`PRODUCTION_CONFIRMATION_VAR`] may hold. Matched exactly, so
/// that a reflexive `=1` or `=true` does not arm live trading.
pub const PRODUCTION_CONFIRMATION_VALUE: &str = "I_UNDERSTAND_THE_RISK";

/// Exchange API credentials.
///
/// The secrets are wrapped in [`SecretString`], which has no `Display`, no
/// `Serialize`, and a redacting `Debug`. The hand-written [`fmt::Debug`] impl
/// below means no derive can ever accidentally start printing these, and the
/// absence of `Clone`/`Serialize` keeps copies from spreading.
pub struct Credentials {
    pub api_key: SecretString,
    pub api_secret: SecretString,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .finish()
    }
}

/// Load credentials from the process environment.
///
/// Milestone 1 reads the environment only; AWS Secrets Manager arrives later.
///
/// # Errors
///
/// Returns [`Error::MissingEnvVar`] if either variable is absent and
/// [`Error::EmptyEnvVar`] if either is present but blank. There is no fallback
/// and no default: without credentials the bot does not start.
pub fn load_credentials() -> Result<Credentials, Error> {
    from_env(env_lookup)
}

/// Whether the operator has explicitly armed production trading.
///
/// True only for an exact match on [`PRODUCTION_CONFIRMATION_VALUE`]. Anything
/// else - unset, empty, `1`, `true`, wrong case, stray whitespace - is false.
#[must_use]
pub fn production_confirmed() -> bool {
    confirmed_from_env(env_lookup)
}

fn env_lookup(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// The pure core of [`load_credentials`], with the environment injected so it
/// can be tested without mutating process-global state.
fn from_env(lookup: impl Fn(&str) -> Option<String>) -> Result<Credentials, Error> {
    Ok(Credentials {
        api_key: required(&lookup, API_KEY_VAR)?,
        api_secret: required(&lookup, API_SECRET_VAR)?,
    })
}

fn required(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
) -> Result<SecretString, Error> {
    match lookup(name) {
        None => Err(Error::MissingEnvVar { name }),
        Some(v) if v.trim().is_empty() => Err(Error::EmptyEnvVar { name }),
        Some(v) => Ok(SecretString::from(v)),
    }
}

/// The pure core of [`production_confirmed`].
fn confirmed_from_env(lookup: impl Fn(&str) -> Option<String>) -> bool {
    lookup(PRODUCTION_CONFIRMATION_VAR).is_some_and(|v| v == PRODUCTION_CONFIRMATION_VALUE)
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    /// An env lookup backed by a fixed list, so tests never touch process env.
    fn fake<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    fn both(key: &str, secret: &str) -> Result<Credentials, Error> {
        from_env(fake(&[(API_KEY_VAR, key), (API_SECRET_VAR, secret)]))
    }

    #[test]
    fn loads_both_credentials_from_the_environment() {
        let creds = both("my-key", "my-secret").expect("both vars present");
        assert_eq!(creds.api_key.expose_secret(), "my-key");
        assert_eq!(creds.api_secret.expose_secret(), "my-secret");
    }

    #[test]
    fn missing_api_key_fails_loudly() {
        let err = from_env(fake(&[(API_SECRET_VAR, "s")])).expect_err("key is missing");
        assert!(matches!(err, Error::MissingEnvVar { name } if name == API_KEY_VAR));
        let msg = err.to_string();
        assert!(
            msg.contains(API_KEY_VAR),
            "message must name the variable: {msg}"
        );
        assert!(
            msg.contains(".env.example"),
            "message must say where to look: {msg}"
        );
    }

    #[test]
    fn missing_api_secret_fails_loudly() {
        let err = from_env(fake(&[(API_KEY_VAR, "k")])).expect_err("secret is missing");
        assert!(matches!(err, Error::MissingEnvVar { name } if name == API_SECRET_VAR));
        assert!(err.to_string().contains(API_SECRET_VAR));
    }

    #[test]
    fn both_missing_reports_the_key_first_and_does_not_panic() {
        let err = from_env(fake(&[])).expect_err("nothing is set");
        assert!(matches!(err, Error::MissingEnvVar { name } if name == API_KEY_VAR));
    }

    #[test]
    fn blank_credentials_are_rejected_rather_than_used() {
        for blank in ["", "   ", "\t\n"] {
            let err = both(blank, "s").expect_err("blank key must be rejected");
            assert!(matches!(err, Error::EmptyEnvVar { name } if name == API_KEY_VAR));
            let err = both("k", blank).expect_err("blank secret must be rejected");
            assert!(matches!(err, Error::EmptyEnvVar { name } if name == API_SECRET_VAR));
        }
    }

    #[test]
    fn debug_output_never_contains_the_secret_values() {
        let creds = both("SUPER_SECRET_KEY", "SUPER_SECRET_VALUE").expect("valid");

        for rendered in [format!("{creds:?}"), format!("{creds:#?}")] {
            assert!(
                !rendered.contains("SUPER_SECRET_KEY"),
                "api key leaked into Debug: {rendered}"
            );
            assert!(
                !rendered.contains("SUPER_SECRET_VALUE"),
                "api secret leaked into Debug: {rendered}"
            );
            assert!(
                rendered.contains("<redacted>"),
                "expected redaction: {rendered}"
            );
        }
    }

    #[test]
    fn debug_of_the_inner_secret_fields_is_also_redacted() {
        // Belt and braces: even if someone logs a field directly, bypassing the
        // Credentials Debug impl above, SecretString must not reveal anything.
        let creds = both("SUPER_SECRET_KEY", "SUPER_SECRET_VALUE").expect("valid");
        let key = format!("{:?}", creds.api_key);
        let secret = format!("{:?}", creds.api_secret);
        assert!(
            !key.contains("SUPER_SECRET_KEY"),
            "SecretString leaked: {key}"
        );
        assert!(
            !secret.contains("SUPER_SECRET_VALUE"),
            "SecretString leaked: {secret}"
        );
    }

    #[test]
    fn production_is_armed_only_by_the_exact_confirmation_value() {
        assert!(confirmed_from_env(fake(&[(
            PRODUCTION_CONFIRMATION_VAR,
            PRODUCTION_CONFIRMATION_VALUE
        )])));
    }

    #[test]
    fn production_stays_disarmed_for_every_near_miss() {
        for sloppy in [
            "",
            "1",
            "true",
            "yes",
            "i_understand_the_risk",
            "I_UNDERSTAND_THE_RISKS",
            " I_UNDERSTAND_THE_RISK",
            "I_UNDERSTAND_THE_RISK ",
        ] {
            assert!(
                !confirmed_from_env(fake(&[(PRODUCTION_CONFIRMATION_VAR, sloppy)])),
                "`{sloppy}` must not arm production"
            );
        }
        assert!(
            !confirmed_from_env(fake(&[])),
            "unset must not arm production"
        );
    }
}
