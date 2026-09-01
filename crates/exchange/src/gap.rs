//! Per-stream sequence tracking. **Pure**, like [`crate::normalize`], and for
//! the same reason: a replay must reconstruct exactly the gaps that were seen
//! live, so this may not read a clock, a socket, or any global state.
//!
//! # Why one rule does not fit both streams
//!
//! The obvious implementation - "flag any id that is not the previous one plus
//! one" - is wrong for half of what we subscribe to, and wrong in the direction
//! that destroys trust in the alert:
//!
//! - `@trade` carries `t`, the per-symbol trade id. It increments by exactly one
//!   per trade, so a jump is a real gap and the number of missed messages is
//!   countable. [`SeqPolicy::Contiguous`].
//! - `@bookTicker` carries `u`, the *order book* updateId. It counts book
//!   updates rather than pushed messages, so it increases by arbitrary amounts
//!   as a matter of course. [`SeqPolicy::Monotonic`]. Applying the contiguous
//!   rule here would fire on nearly every message, which is worse than no
//!   alerting at all: it teaches an operator to ignore the one signal that says
//!   data is missing.
//!
//! Which policy applied is therefore not an implementation detail - it is part
//! of what a gap *means*, and it is recorded in every gap marker so that replay
//! reads the semantics off the file instead of guessing them.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::normalize::{Seq, SeqPolicy};

/// What kind of evidence a gap marker records.
///
/// These are not interchangeable, and neither are the policies they occur under:
/// a `Missed` under [`SeqPolicy::Contiguous`] states how many messages were lost,
/// while the same numbers under [`SeqPolicy::Monotonic`] would state nothing at
/// all. That is why both travel in the marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GapKind {
    /// A contiguous stream skipped ids. `missing` says how many.
    Missed,
    /// The id repeated or went backwards. Evidence of reordering or a reset,
    /// detectable under either policy, countable under neither.
    Regressed,
    /// The first message on this stream after a reconnect. Whatever happened
    /// during the outage happened; under a monotonic policy we cannot say how
    /// much, which is exactly the fact worth recording.
    Outage,
}

/// One gap, in the form that goes into a recording and into
/// `domain::IngestMsg::Gap`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapDetail {
    /// The Binance stream name, e.g. `btcusdt@trade`.
    pub stream: String,
    /// The sequence semantics that applied. Load-bearing: without it a replay
    /// cannot tell a countable gap from an uncountable one.
    pub policy: SeqPolicy,
    pub kind: GapKind,
    /// The last id seen before the gap.
    pub from: i64,
    /// The first id seen after it.
    pub to: i64,
    /// How many messages were lost, when that is knowable at all. `None` under
    /// [`SeqPolicy::Monotonic`], where the id counts something other than
    /// messages. Always serialised, as an explicit `null`, so a recording never
    /// leaves a reader to infer the difference from an absent key.
    #[serde(default)]
    pub missing: Option<u64>,
}

/// Tracks the last sequence id seen on each stream and reports gaps.
///
/// Deterministic: driven only by the calls made to it, in order. Feeding it the
/// same observations from a recording produces the same gaps it produced live,
/// which is the property the round-trip test pins.
#[derive(Debug, Default)]
pub struct SeqTracker {
    last: HashMap<String, i64>,
    /// Streams whose next message spans a connection outage.
    resuming: HashSet<String>,
}

impl SeqTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Note that the connection dropped. The next message on every stream we
    /// have already seen spans the outage and is reported as such.
    ///
    /// Streams never seen before are not marked: there is no `from` to report,
    /// and inventing one would put a fabricated range into the archive.
    pub fn mark_outage(&mut self) {
        self.resuming.extend(self.last.keys().cloned());
    }

    /// The last id seen on a stream, if any. For logging and tests.
    #[must_use]
    pub fn last_seen(&self, stream: &str) -> Option<i64> {
        self.last.get(stream).copied()
    }

    /// Observe one message's sequence id, returning a gap if it evidences one.
    ///
    /// The first message on a stream never reports a gap: with nothing to
    /// compare against, any range we produced would be invented.
    pub fn observe(&mut self, stream: &str, seq: Seq) -> Option<GapDetail> {
        let previous = self.last.insert(stream.to_owned(), seq.id);
        let after_outage = self.resuming.remove(stream);

        let from = previous?;
        let detail = |kind, missing| {
            Some(GapDetail {
                stream: stream.to_owned(),
                policy: seq.policy,
                kind,
                from,
                to: seq.id,
                missing,
            })
        };

        if seq.id <= from {
            // A repeat or a decrease. Under either policy this is reordering, a
            // server-side reset, or a duplicate - never something to count.
            return detail(GapKind::Regressed, None);
        }

        // Only a contiguous id counts messages, so only it can count losses.
        let missed = match seq.policy {
            SeqPolicy::Contiguous => Some(
                u64::try_from(seq.id - from - 1)
                    .expect("id > from was just checked, so the difference is non-negative"),
            ),
            SeqPolicy::Monotonic => None,
        };

        if after_outage {
            // Recorded even when `missed` is zero: "we reconnected and lost
            // nothing" is a different fact from "we never checked", and only one
            // of them belongs in an archive as silence.
            return detail(GapKind::Outage, missed);
        }

        match missed {
            Some(0) | None => None,
            Some(n) => detail(GapKind::Missed, Some(n)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contiguous(id: i64) -> Seq {
        Seq {
            id,
            policy: SeqPolicy::Contiguous,
        }
    }

    fn monotonic(id: i64) -> Seq {
        Seq {
            id,
            policy: SeqPolicy::Monotonic,
        }
    }

    const TRADE: &str = "btcusdt@trade";
    const BOOK: &str = "btcusdt@bookTicker";

    #[test]
    fn the_first_message_on_a_stream_never_reports_a_gap() {
        let mut t = SeqTracker::new();
        assert_eq!(t.observe(TRADE, contiguous(1_000)), None);
        assert_eq!(t.observe(BOOK, monotonic(50)), None);
        assert_eq!(t.last_seen(TRADE), Some(1_000));
    }

    #[test]
    fn a_contiguous_stream_counts_exactly_what_it_missed() {
        let mut t = SeqTracker::new();
        t.observe(TRADE, contiguous(100));
        assert_eq!(t.observe(TRADE, contiguous(101)), None, "no gap on +1");

        let gap = t.observe(TRADE, contiguous(137)).expect("a gap");
        assert_eq!(
            gap,
            GapDetail {
                stream: TRADE.to_owned(),
                policy: SeqPolicy::Contiguous,
                kind: GapKind::Missed,
                from: 101,
                to: 137,
                missing: Some(35),
            }
        );
    }

    #[test]
    fn a_monotonic_stream_never_reports_a_forward_jump() {
        // The whole point of the policy split: bookTicker's updateId leaps
        // around normally, and calling that a gap would make the alert useless.
        let mut t = SeqTracker::new();
        t.observe(BOOK, monotonic(1));
        for id in [2, 9, 400, 401, 999_999] {
            assert_eq!(
                t.observe(BOOK, monotonic(id)),
                None,
                "a forward jump to {id} is normal for an order book updateId"
            );
        }
    }

    #[test]
    fn the_same_jump_means_different_things_under_the_two_policies() {
        // Identical numbers, opposite verdicts. This is why the policy has to be
        // recorded rather than assumed.
        let mut contiguous_tracker = SeqTracker::new();
        contiguous_tracker.observe("s@trade", contiguous(10));
        let counted = contiguous_tracker
            .observe("s@trade", contiguous(20))
            .expect("a contiguous stream reports this");
        assert_eq!(counted.kind, GapKind::Missed);
        assert_eq!(counted.missing, Some(9));

        let mut monotonic_tracker = SeqTracker::new();
        monotonic_tracker.observe("s@bookTicker", monotonic(10));
        assert_eq!(
            monotonic_tracker.observe("s@bookTicker", monotonic(20)),
            None,
            "the identical jump is unremarkable under a monotonic policy"
        );
    }

    #[test]
    fn a_regression_is_reported_under_both_policies_and_counted_under_neither() {
        for (stream, seq) in [
            (TRADE, contiguous as fn(i64) -> Seq),
            (BOOK, monotonic as fn(i64) -> Seq),
        ] {
            let mut t = SeqTracker::new();
            t.observe(stream, seq(500));

            let backwards = t.observe(stream, seq(499)).expect("a regression");
            assert_eq!(backwards.kind, GapKind::Regressed);
            assert_eq!(backwards.from, 500);
            assert_eq!(backwards.to, 499);
            assert_eq!(
                backwards.missing, None,
                "a regression is never a countable loss"
            );

            // A repeat is a regression too: the id did not advance.
            t.observe(stream, seq(600));
            let repeat = t.observe(stream, seq(600)).expect("a repeat");
            assert_eq!(repeat.kind, GapKind::Regressed);
            assert_eq!((repeat.from, repeat.to), (600, 600));
        }
    }

    #[test]
    fn a_reconnect_flags_the_outage_on_every_stream_it_had_seen() {
        let mut t = SeqTracker::new();
        t.observe(TRADE, contiguous(100));
        t.observe(BOOK, monotonic(7_000));

        t.mark_outage();

        // A contiguous stream can still say how much it lost across the outage.
        let trade_gap = t.observe(TRADE, contiguous(105)).expect("outage gap");
        assert_eq!(trade_gap.kind, GapKind::Outage);
        assert_eq!(trade_gap.policy, SeqPolicy::Contiguous);
        assert_eq!((trade_gap.from, trade_gap.to), (100, 105));
        assert_eq!(trade_gap.missing, Some(4));

        // A monotonic one cannot, and records that it cannot.
        let book_gap = t.observe(BOOK, monotonic(9_000)).expect("outage gap");
        assert_eq!(book_gap.kind, GapKind::Outage);
        assert_eq!(book_gap.policy, SeqPolicy::Monotonic);
        assert_eq!(book_gap.missing, None, "unknowable, and recorded as such");

        // The outage is spent: the next message is judged normally again.
        assert_eq!(t.observe(TRADE, contiguous(106)), None);
        assert_eq!(t.observe(BOOK, monotonic(12_000)), None);
    }

    #[test]
    fn an_outage_that_lost_nothing_is_still_recorded() {
        // "We reconnected and lost nothing" is a different fact from "we never
        // checked", and only one of them may look like silence in the archive.
        let mut t = SeqTracker::new();
        t.observe(TRADE, contiguous(10));
        t.mark_outage();

        let gap = t
            .observe(TRADE, contiguous(11))
            .expect("outage still recorded");
        assert_eq!(gap.kind, GapKind::Outage);
        assert_eq!(gap.missing, Some(0));
    }

    #[test]
    fn an_outage_does_not_invent_a_range_for_a_stream_never_seen() {
        let mut t = SeqTracker::new();
        t.observe(TRADE, contiguous(10));
        t.mark_outage();
        // BOOK had never produced a message, so there is no `from` to report.
        assert_eq!(t.observe(BOOK, monotonic(1)), None);
    }

    #[test]
    fn streams_are_tracked_independently() {
        let mut t = SeqTracker::new();
        t.observe(TRADE, contiguous(100));
        t.observe("ethusdt@trade", contiguous(900));

        assert_eq!(t.observe(TRADE, contiguous(101)), None);
        let gap = t
            .observe("ethusdt@trade", contiguous(950))
            .expect("only eth gapped");
        assert_eq!(gap.stream, "ethusdt@trade");
        assert_eq!(gap.missing, Some(49));
        assert_eq!(t.last_seen(TRADE), Some(101));
    }

    #[test]
    fn tracking_is_deterministic_over_a_replayed_observation_sequence() {
        // The property the recording format exists to preserve: the same
        // observations in the same order produce the same gaps, every time.
        let script: Vec<(&str, Seq)> = vec![
            (TRADE, contiguous(1)),
            (BOOK, monotonic(10)),
            (TRADE, contiguous(2)),
            (BOOK, monotonic(4_000)),
            (TRADE, contiguous(40)),
            (BOOK, monotonic(3_999)),
            (TRADE, contiguous(41)),
        ];

        let run = || {
            let mut t = SeqTracker::new();
            script
                .iter()
                .filter_map(|(s, seq)| t.observe(s, *seq))
                .collect::<Vec<_>>()
        };

        let first = run();
        assert_eq!(first.len(), 2, "one missed trade run, one book regression");
        assert_eq!(first[0].kind, GapKind::Missed);
        assert_eq!(first[1].kind, GapKind::Regressed);
        for _ in 0..10 {
            assert_eq!(run(), first);
        }
    }
}
