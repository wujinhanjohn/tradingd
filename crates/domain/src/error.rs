/// Errors produced by domain-level construction and validation.
///
/// The domain fails closed: a value that cannot be proven valid is rejected
/// rather than coerced into something plausible.
#[derive(thiserror::Error, Debug)]
pub enum DomainError {
    #[error("invalid symbol: {0}")]
    InvalidSymbol(String),
}
