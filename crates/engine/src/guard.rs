//! The environment guard: the last thing between a config file and real money.

use settings::{Config, Env, PRODUCTION_CONFIRMATION_VALUE, PRODUCTION_CONFIRMATION_VAR};

use crate::Error;

/// Refuse to start unless the configured environment is one the operator has
/// explicitly armed.
///
/// Testnet needs nothing. Production additionally requires
/// `ALLOW_PRODUCTION=I_UNDERSTAND_THE_RISK` in the process environment, matched
/// exactly. That variable is deliberately not `APP_`-prefixed, so no checked-in
/// config file can arm live trading on its own.
///
/// # Errors
///
/// Returns [`Error::ProductionNotConfirmed`] if the config asks for production
/// and the environment does not confirm it.
pub fn guard_environment(config: &Config) -> Result<(), Error> {
    check(config.environment, settings::production_confirmed())
}

/// The pure decision, with the confirmation flag injected so it can be tested
/// without mutating process-global state.
fn check(environment: Env, confirmed: bool) -> Result<(), Error> {
    match environment {
        Env::Testnet => {
            tracing::info!(environment = %environment, "environment guard passed");
            Ok(())
        }
        Env::Production if confirmed => {
            // Loud on purpose. If this scrolls past unnoticed, the guard has failed
            // at its real job even though it returned Ok.
            tracing::warn!(
                environment = %environment,
                "*** PRODUCTION MODE - ORDERS WILL USE REAL FUNDS ***"
            );
            Ok(())
        }
        Env::Production => {
            // Fail closed: an unconfirmed production config does nothing at all.
            tracing::error!(
                environment = %environment,
                required_var = PRODUCTION_CONFIRMATION_VAR,
                "refusing to start: production is not confirmed"
            );
            Err(Error::ProductionNotConfirmed {
                var: PRODUCTION_CONFIRMATION_VAR,
                value: PRODUCTION_CONFIRMATION_VALUE,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn testnet_starts_without_any_confirmation() {
        assert!(check(Env::Testnet, false).is_ok());
    }

    #[test]
    fn testnet_is_unaffected_by_a_stray_confirmation() {
        // Leaving ALLOW_PRODUCTION set must not change testnet behaviour.
        assert!(check(Env::Testnet, true).is_ok());
    }

    #[test]
    fn production_without_confirmation_refuses_to_start() {
        let err = check(Env::Production, false).expect_err("must refuse");
        assert!(
            matches!(&err, Error::ProductionNotConfirmed { var, value }
                if *var == PRODUCTION_CONFIRMATION_VAR && *value == PRODUCTION_CONFIRMATION_VALUE),
            "got {err:?}"
        );
    }

    #[test]
    fn the_refusal_tells_the_operator_exactly_what_to_do() {
        let msg = check(Env::Production, false)
            .expect_err("must refuse")
            .to_string();
        assert!(msg.contains(PRODUCTION_CONFIRMATION_VAR), "{msg}");
        assert!(msg.contains(PRODUCTION_CONFIRMATION_VALUE), "{msg}");
        assert!(
            msg.contains("testnet"),
            "should offer the safe way out: {msg}"
        );
    }

    #[test]
    fn production_with_confirmation_is_allowed() {
        assert!(check(Env::Production, true).is_ok());
    }
}
