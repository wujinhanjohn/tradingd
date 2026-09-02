//! What we subscribed to, and the only route by which a stream's sequence
//! semantics reach gap detection.
//!
//! # Why this module exists at all
//!
//! [`crate::gap::SeqTracker`] takes a [`Seq`], which carries a [`SeqPolicy`]
//! alongside the id. That is right for the tracker - a recording has to say
//! which semantics applied - but on its own it leaves a policy *argument* on the
//! hot path, and an argument is something a connection layer can get wrong.
//! Passing `SeqPolicy::Contiguous` for a `@bookTicker` stream would compile, and
//! would then fire a gap alert on nearly every message until an operator learned
//! to ignore the one signal that says data is missing.
//!
//! So the connection layer never handles a `SeqPolicy` at all. It handles a
//! [`StreamSet`] built from [`StreamKind`]s, and it observes raw `i64` ids
//! through [`StreamTracker::observe`], which looks the policy up from the
//! subscription that declared it. There is no parameter to pass and no default
//! to fall back on: the policy is a property of the subscription, fixed when the
//! set is built, and [`Subscription`] has no field for it - only
//! [`StreamKind::seq_policy`], which is exhaustive over the kinds.
//!
//! The one place a policy still arrives from outside is [`crate::normalize`],
//! which derives it from the same function. [`StreamTracker::observe`]
//! cross-checks the two and refuses if they ever disagree, so the invariant is
//! asserted rather than assumed.

use std::collections::{BTreeSet, HashMap};

use domain::Symbol;

use crate::gap::{GapDetail, SeqTracker};
use crate::normalize::{Seq, SeqPolicy, StreamId, StreamKind};

/// One subscribed stream: a symbol, a kind, and the wire name they produce.
///
/// Note the absence of a `policy` field. There is nowhere to put a wrong one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscription {
    id: StreamId,
    name: String,
}

impl Subscription {
    #[must_use]
    pub fn new(symbol: Symbol, kind: StreamKind) -> Self {
        Self {
            name: kind.stream_name(&symbol),
            id: StreamId { symbol, kind },
        }
    }

    /// The Binance stream name, e.g. `btcusdt@bookTicker`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn symbol(&self) -> &Symbol {
        &self.id.symbol
    }

    #[must_use]
    pub fn kind(&self) -> StreamKind {
        self.id.kind
    }

    /// The sequence semantics this stream's ordering id offers.
    ///
    /// Derived, never stored: it is whatever the kind says it is.
    #[must_use]
    pub fn policy(&self) -> SeqPolicy {
        self.id.kind.seq_policy()
    }
}

/// Refusals to build a subscription set. Both mean "do not start".
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum SubscriptionError {
    #[error(
        "no streams to subscribe to. A market source that subscribes to nothing \
         would connect, stay silent, and look healthy; refusing to start instead"
    )]
    Empty,

    #[error(
        "cannot subscribe to `{stream}` twice. A duplicate would be tracked once \
         and counted twice, so the configuration is rejected rather than \
         quietly deduplicated"
    )]
    Duplicate { stream: String },
}

/// The full set of streams one connection subscribes to.
///
/// Non-empty and duplicate-free by construction, and ordered, so the SUBSCRIBE
/// request a given configuration produces is byte-identical run to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamSet {
    subscriptions: Vec<Subscription>,
}

impl StreamSet {
    /// Build the cross product of `symbols` x `kinds`.
    ///
    /// # Errors
    ///
    /// [`SubscriptionError::Empty`] if either side is empty, and
    /// [`SubscriptionError::Duplicate`] if the inputs name the same stream twice.
    pub fn new(symbols: &[Symbol], kinds: &[StreamKind]) -> Result<Self, SubscriptionError> {
        let mut subscriptions = Vec::with_capacity(symbols.len() * kinds.len());
        for symbol in symbols {
            for kind in kinds {
                subscriptions.push(Subscription::new(symbol.clone(), *kind));
            }
        }
        Self::from_subscriptions(subscriptions)
    }

    /// Build from explicit subscriptions, preserving their order.
    ///
    /// # Errors
    ///
    /// As [`StreamSet::new`].
    pub fn from_subscriptions(subscriptions: Vec<Subscription>) -> Result<Self, SubscriptionError> {
        if subscriptions.is_empty() {
            return Err(SubscriptionError::Empty);
        }

        let mut seen = BTreeSet::new();
        for subscription in &subscriptions {
            if !seen.insert(subscription.name()) {
                return Err(SubscriptionError::Duplicate {
                    stream: subscription.name().to_owned(),
                });
            }
        }

        Ok(Self { subscriptions })
    }

    #[must_use]
    pub fn subscriptions(&self) -> &[Subscription] {
        &self.subscriptions
    }

    /// The stream names, in subscription order. This is the `params` array of
    /// the SUBSCRIBE request and the `streams` list in a recording header.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.subscriptions
            .iter()
            .map(|s| s.name().to_owned())
            .collect()
    }

    /// The distinct symbols, in first-seen order. Names the recording file.
    #[must_use]
    pub fn symbols(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for subscription in &self.subscriptions {
            let symbol = subscription.symbol().as_str();
            if !out.iter().any(|s| s == symbol) {
                out.push(symbol.to_owned());
            }
        }
        out
    }

    /// The subscription for a stream name, or `None` if we never asked for it.
    #[must_use]
    pub fn get(&self, stream: &str) -> Option<&Subscription> {
        self.subscriptions.iter().find(|s| s.name() == stream)
    }
}

/// A message arrived on a stream, or claimed semantics, that the subscription
/// set does not account for. Both are refusals to track it.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum TrackError {
    #[error(
        "received a message on `{stream}`, which we never subscribed to. \
         Refusing to track a stream we cannot state the sequence semantics of"
    )]
    UnknownStream { stream: String },

    #[error(
        "`{stream}` is declared `{declared:?}` by its subscription but the \
         parsed message claims `{observed:?}`. These are not interchangeable, \
         so the message is dropped rather than judged under the wrong rule"
    )]
    PolicyDisagreement {
        stream: String,
        declared: SeqPolicy,
        observed: SeqPolicy,
    },
}

/// Gap detection over a fixed set of subscriptions.
///
/// Wraps [`SeqTracker`] and supplies the policy itself, so the caller has no
/// policy argument to get wrong. Construction requires the [`StreamSet`]; there
/// is no `Default` and no way to track a stream nobody declared.
#[derive(Debug)]
pub struct StreamTracker {
    /// Fixed at construction from the declared subscriptions. Never written to
    /// again, and never consulted for a stream that is not in it.
    policies: HashMap<String, SeqPolicy>,
    inner: SeqTracker,
}

impl StreamTracker {
    /// Build a tracker that knows exactly the streams `set` declares.
    #[must_use]
    pub fn new(set: &StreamSet) -> Self {
        Self {
            policies: set
                .subscriptions()
                .iter()
                .map(|s| (s.name().to_owned(), s.policy()))
                .collect(),
            inner: SeqTracker::new(),
        }
    }

    /// The declared policy for a stream, if it was subscribed.
    #[must_use]
    pub fn policy_of(&self, stream: &str) -> Option<SeqPolicy> {
        self.policies.get(stream).copied()
    }

    /// Note that the connection dropped, so the next message on each stream
    /// spans the outage.
    pub fn mark_outage(&mut self) {
        self.inner.mark_outage();
    }

    /// The last id seen on a stream, if any.
    #[must_use]
    pub fn last_seen(&self, stream: &str) -> Option<i64> {
        self.inner.last_seen(stream)
    }

    /// Observe one message's ordering id under the policy its subscription
    /// declared.
    ///
    /// The id arrives as a bare `i64` and `observed` is the policy the pure
    /// normalizer independently derived. Both come from
    /// [`StreamKind::seq_policy`], so they agree by construction - and the check
    /// below is what turns "by construction" into something the compiler-adjacent
    /// tests can hold us to, rather than a comment.
    ///
    /// # Errors
    ///
    /// [`TrackError::UnknownStream`] for a stream we never subscribed to, and
    /// [`TrackError::PolicyDisagreement`] if the declared and derived semantics
    /// ever diverge. Neither is tracked; a message we cannot judge correctly is
    /// dropped rather than judged wrongly.
    pub fn observe(
        &mut self,
        stream: &str,
        id: i64,
        observed: SeqPolicy,
    ) -> Result<Option<GapDetail>, TrackError> {
        let declared =
            self.policies
                .get(stream)
                .copied()
                .ok_or_else(|| TrackError::UnknownStream {
                    stream: stream.to_owned(),
                })?;

        if declared != observed {
            return Err(TrackError::PolicyDisagreement {
                stream: stream.to_owned(),
                declared,
                observed,
            });
        }

        Ok(self.inner.observe(
            stream,
            Seq {
                id,
                policy: declared,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).expect("valid symbol")
    }

    fn set() -> StreamSet {
        StreamSet::new(
            &[symbol("BTCUSDT"), symbol("ETHUSDT")],
            &[StreamKind::BookTicker, StreamKind::Trade],
        )
        .expect("a valid stream set")
    }

    #[test]
    fn a_subscription_derives_its_policy_from_its_kind() {
        let book = Subscription::new(symbol("BTCUSDT"), StreamKind::BookTicker);
        assert_eq!(book.name(), "btcusdt@bookTicker");
        assert_eq!(book.policy(), SeqPolicy::Monotonic);

        let trade = Subscription::new(symbol("BTCUSDT"), StreamKind::Trade);
        assert_eq!(trade.name(), "btcusdt@trade");
        assert_eq!(trade.policy(), SeqPolicy::Contiguous);
    }

    #[test]
    fn every_stream_kind_declares_a_policy() {
        // If a later milestone adds a kind, `seq_policy` is exhaustive over the
        // enum, so this cannot silently acquire a default.
        for kind in StreamKind::ALL {
            let subscription = Subscription::new(symbol("BTCUSDT"), *kind);
            assert_eq!(
                subscription.policy(),
                kind.seq_policy(),
                "a subscription's policy is its kind's policy, never a choice"
            );
        }
    }

    #[test]
    fn a_stream_set_is_the_cross_product_in_a_stable_order() {
        let set = set();
        assert_eq!(
            set.names(),
            vec![
                "btcusdt@bookTicker",
                "btcusdt@trade",
                "ethusdt@bookTicker",
                "ethusdt@trade",
            ]
        );
        assert_eq!(set.symbols(), vec!["BTCUSDT", "ETHUSDT"]);
    }

    #[test]
    fn an_empty_subscription_set_refuses_to_build() {
        // A source that subscribes to nothing connects, stays silent, and looks
        // perfectly healthy. That is the failure this project exists to refuse.
        assert_eq!(
            StreamSet::new(&[], &[StreamKind::Trade]),
            Err(SubscriptionError::Empty)
        );
        assert_eq!(
            StreamSet::new(&[symbol("BTCUSDT")], &[]),
            Err(SubscriptionError::Empty)
        );
    }

    #[test]
    fn a_duplicate_stream_refuses_rather_than_being_deduplicated() {
        let err = StreamSet::from_subscriptions(vec![
            Subscription::new(symbol("BTCUSDT"), StreamKind::Trade),
            Subscription::new(symbol("BTCUSDT"), StreamKind::Trade),
        ])
        .expect_err("a duplicate is a configuration error");
        assert_eq!(
            err,
            SubscriptionError::Duplicate {
                stream: "btcusdt@trade".to_owned()
            }
        );
    }

    #[test]
    fn the_tracker_takes_its_policy_from_the_subscription_not_the_caller() {
        // The load-bearing test for this module. `observe` has no policy to
        // choose: the same numeric jump is a countable gap on the trade stream
        // and unremarkable on the book ticker, decided entirely by which stream
        // it arrived on.
        let set = set();
        let mut tracker = StreamTracker::new(&set);

        tracker
            .observe("btcusdt@trade", 100, SeqPolicy::Contiguous)
            .expect("subscribed");
        let gap = tracker
            .observe("btcusdt@trade", 110, SeqPolicy::Contiguous)
            .expect("subscribed")
            .expect("a contiguous stream reports this");
        assert_eq!(gap.policy, SeqPolicy::Contiguous);
        assert_eq!(gap.missing, Some(9));

        tracker
            .observe("btcusdt@bookTicker", 100, SeqPolicy::Monotonic)
            .expect("subscribed");
        assert_eq!(
            tracker
                .observe("btcusdt@bookTicker", 110, SeqPolicy::Monotonic)
                .expect("subscribed"),
            None,
            "the identical jump is normal for an order book updateId"
        );
    }

    #[test]
    fn the_policy_of_every_subscribed_stream_is_fixed_at_construction() {
        let set = set();
        let tracker = StreamTracker::new(&set);
        for subscription in set.subscriptions() {
            assert_eq!(
                tracker.policy_of(subscription.name()),
                Some(subscription.policy())
            );
        }
        assert_eq!(tracker.policy_of("btcusdt@depth"), None);
    }

    #[test]
    fn a_message_on_an_unsubscribed_stream_is_refused_not_tracked() {
        let set =
            StreamSet::new(&[symbol("BTCUSDT")], &[StreamKind::Trade]).expect("a valid stream set");
        let mut tracker = StreamTracker::new(&set);

        assert_eq!(
            tracker.observe("ethusdt@trade", 1, SeqPolicy::Contiguous),
            Err(TrackError::UnknownStream {
                stream: "ethusdt@trade".to_owned()
            })
        );
        assert_eq!(
            tracker.last_seen("ethusdt@trade"),
            None,
            "a refused stream leaves no state behind"
        );
    }

    #[test]
    fn a_policy_disagreement_drops_the_message_rather_than_judging_it_wrongly() {
        // Unreachable through `normalize`, which derives the policy from the
        // same function the subscription does. Asserted anyway: this is the
        // invariant the whole module exists to hold.
        let set = StreamSet::new(&[symbol("BTCUSDT")], &[StreamKind::BookTicker])
            .expect("a valid stream set");
        let mut tracker = StreamTracker::new(&set);

        assert_eq!(
            tracker.observe("btcusdt@bookTicker", 1, SeqPolicy::Contiguous),
            Err(TrackError::PolicyDisagreement {
                stream: "btcusdt@bookTicker".to_owned(),
                declared: SeqPolicy::Monotonic,
                observed: SeqPolicy::Contiguous,
            })
        );
        assert_eq!(tracker.last_seen("btcusdt@bookTicker"), None);
    }

    #[test]
    fn an_outage_is_flagged_under_each_streams_own_policy() {
        let set = set();
        let mut tracker = StreamTracker::new(&set);
        tracker
            .observe("btcusdt@trade", 10, SeqPolicy::Contiguous)
            .expect("subscribed");
        tracker
            .observe("btcusdt@bookTicker", 10, SeqPolicy::Monotonic)
            .expect("subscribed");

        tracker.mark_outage();

        let trade = tracker
            .observe("btcusdt@trade", 20, SeqPolicy::Contiguous)
            .expect("subscribed")
            .expect("an outage gap");
        assert_eq!(trade.policy, SeqPolicy::Contiguous);
        assert_eq!(trade.missing, Some(9), "countable across the outage");

        let book = tracker
            .observe("btcusdt@bookTicker", 20, SeqPolicy::Monotonic)
            .expect("subscribed")
            .expect("an outage gap");
        assert_eq!(book.policy, SeqPolicy::Monotonic);
        assert_eq!(book.missing, None, "uncountable, and recorded as such");
    }
}
