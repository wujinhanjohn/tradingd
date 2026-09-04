//! The filter book: the symbol rules currently in force, and how old they are.
//!
//! # Freshness is not caching
//!
//! Symbol filters are exchange state we do not control and that *changes*. A
//! book fetched once at startup and held forever is a latent bug: Binance moves
//! a tick size, and the quantizer keeps producing orders that were valid
//! yesterday. So the age of the book travels with it, and
//! [`FilterBook::ensure_fresh`] is the contract by which a caller refuses to act
//! on rules it can no longer vouch for.
//!
//! The teeth land in milestone 6, where the order path calls `ensure_fresh`
//! before it quantizes anything. The contract is defined here, now, so that M6
//! consumes it rather than inventing one under order-path pressure - and so the
//! refresh loop can be built and tested against it.

use std::collections::HashMap;

use domain::{Symbol, SymbolFilters};

/// The symbol filters in force, as of a moment.
///
/// Immutable: a refresh publishes a whole new book rather than mutating this
/// one, so a reader can never observe half an update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterBook {
    filters: HashMap<Symbol, SymbolFilters>,
    fetched_at_ns: i64,
}

/// The book is too old to trade against.
///
/// Carries the arithmetic rather than just the verdict, because the operator's
/// next question is always "by how much, and since when".
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error(
    "symbol filters are {}ms old (fetched at {fetched_at_ns}ns, now {now_ns}ns), past the \
     {}ms bound. Refusing to act on rules we can no longer vouch for",
    .age_ns / 1_000_000,
    .max_age_ns / 1_000_000
)]
pub struct FilterStale {
    pub fetched_at_ns: i64,
    pub now_ns: i64,
    /// Negative when the clock has gone backwards, which is itself a refusal.
    pub age_ns: i64,
    pub max_age_ns: i64,
}

impl FilterBook {
    /// Build a book from a completed fetch.
    #[must_use]
    pub fn new(filters: HashMap<Symbol, SymbolFilters>, fetched_at_ns: i64) -> Self {
        Self {
            filters,
            fetched_at_ns,
        }
    }

    /// The rules for one symbol, or `None` if this book does not carry it.
    ///
    /// `None` is a refusal, not a default: there is no "assume no filters".
    #[must_use]
    pub fn get(&self, symbol: &Symbol) -> Option<&SymbolFilters> {
        self.filters.get(symbol)
    }

    /// When this book was fetched, in nanoseconds since the Unix epoch.
    #[must_use]
    pub fn fetched_at_ns(&self) -> i64 {
        self.fetched_at_ns
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// The symbols in this book, in a stable (sorted) order.
    #[must_use]
    pub fn symbols(&self) -> Vec<Symbol> {
        let mut symbols: Vec<Symbol> = self.filters.keys().cloned().collect();
        symbols.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        symbols
    }

    /// The freshness contract: refuse unless this book is younger than
    /// `max_age_ns`.
    ///
    /// `now_ns` is passed in rather than read, exactly as [`crate::normalize`]
    /// takes its ingest time: staleness must be reproducible in a replay, and a
    /// function that reads a clock is not.
    ///
    /// A book from the *future* - a clock that went backwards, an NTP step - is
    /// stale too. We cannot say how old it is, and "cannot say" is a refusal.
    ///
    /// # Errors
    ///
    /// [`FilterStale`], carrying the age and the bound it broke.
    pub fn ensure_fresh(&self, now_ns: i64, max_age_ns: i64) -> Result<(), FilterStale> {
        let age_ns = now_ns.saturating_sub(self.fetched_at_ns);
        if age_ns >= 0 && age_ns < max_age_ns {
            return Ok(());
        }
        Err(FilterStale {
            fetched_at_ns: self.fetched_at_ns,
            now_ns,
            age_ns,
            max_age_ns,
        })
    }
}

#[cfg(test)]
mod tests {
    use domain::{Decimal, Price, Qty, SymbolFilterSpec};

    use super::*;

    const SECOND_NS: i64 = 1_000_000_000;

    fn symbol(name: &str) -> Symbol {
        Symbol::new(name).expect("valid symbol")
    }

    /// `exchange` has no `rust_decimal` of its own - it speaks decimals through
    /// `domain`'s re-export - so test values are parsed exactly, as they arrive
    /// on the wire, rather than written with the `dec!` macro.
    fn dec(text: &str) -> Decimal {
        Decimal::from_str_exact(text).expect("literal decimal")
    }

    fn book(fetched_at_ns: i64) -> FilterBook {
        let btc = symbol("BTCUSDT");
        let filters = SymbolFilters::new(SymbolFilterSpec {
            symbol: btc.clone(),
            tick_size: Price(dec("0.01")),
            min_price: Price(dec("0.01")),
            max_price: Price(dec("1000000")),
            step_size: Qty(dec("0.00001")),
            min_qty: Qty(dec("0.00001")),
            max_qty: Qty(dec("9000")),
            min_notional: dec("5"),
        })
        .expect("filters");
        FilterBook::new(HashMap::from([(btc, filters)]), fetched_at_ns)
    }

    #[test]
    fn a_book_answers_for_the_symbols_it_holds_and_refuses_for_the_rest() {
        let book = book(1_000);
        assert!(book.get(&symbol("BTCUSDT")).is_some());
        assert!(
            book.get(&symbol("ETHUSDT")).is_none(),
            "a symbol we did not fetch has no rules, not empty rules"
        );
        assert_eq!(book.len(), 1);
        assert!(!book.is_empty());
        assert_eq!(book.symbols(), vec![symbol("BTCUSDT")]);
        assert_eq!(book.fetched_at_ns(), 1_000);
    }

    #[test]
    fn freshness_is_measured_against_an_injected_clock_not_a_real_one() {
        let fetched = 1_000 * SECOND_NS;
        let book = book(fetched);
        let max_age = 60 * SECOND_NS;

        assert_eq!(book.ensure_fresh(fetched, max_age), Ok(()), "just fetched");
        assert_eq!(
            book.ensure_fresh(fetched + max_age - 1, max_age),
            Ok(()),
            "one nanosecond inside the bound"
        );

        // Exactly at the bound is already stale: the bound is the age we will
        // still act on, and "exactly max_age old" is not younger than max_age.
        let err = book
            .ensure_fresh(fetched + max_age, max_age)
            .expect_err("at the bound");
        assert_eq!(err.age_ns, max_age);
        assert_eq!(err.max_age_ns, max_age);

        let err = book
            .ensure_fresh(fetched + 10 * max_age, max_age)
            .expect_err("well past the bound");
        assert!(err.to_string().contains("Refusing"), "{err}");
        assert!(
            err.to_string().contains("600000ms"),
            "the message states the age: {err}"
        );
    }

    #[test]
    fn a_book_from_the_future_is_stale_because_we_cannot_say_how_old_it_is() {
        // A clock that steps backwards must not look like an infinitely fresh
        // book. This is the fail-closed reading of "I do not know".
        let book = book(1_000 * SECOND_NS);
        let err = book
            .ensure_fresh(900 * SECOND_NS, 60 * SECOND_NS)
            .expect_err("a negative age is not freshness");
        assert!(err.age_ns < 0, "{err:?}");
    }

    #[test]
    fn a_zero_or_negative_bound_can_never_be_satisfied() {
        let book = book(1_000);
        assert!(book.ensure_fresh(1_000, 0).is_err());
        assert!(book.ensure_fresh(1_000, -1).is_err());
    }
}
