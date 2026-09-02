//! Reconnect pacing: capped exponential backoff, plus jitter.
//!
//! # Why jitter, and why it is not optional
//!
//! Pure exponential backoff makes every client that was dropped by the same
//! event retry at the same instants forever. Binance force-closes connections at
//! the 24 hour mark and drops everyone at once during a `serverShutdown`, so
//! synchronised retries are the normal case rather than the pathological one.
//! Jitter spreads them out, which is why it is a property of this type rather
//! than something a caller may forget to add.
//!
//! # Why the arithmetic is all integers
//!
//! The workspace bans `f32`/`f64` outright (`clippy.toml`) and denies
//! `float_arithmetic`. That ban exists for money, but it applies here too and
//! there is no reason to want an exception: durations are whole milliseconds,
//! the growth factor is a small integer, and the jitter fraction is a percentage.
//! Every operation below saturates rather than wrapping, so an absurd
//! configuration produces a long wait, never a zero one.
//!
//! # Why the randomness is hand-rolled
//!
//! Jitter needs to decorrelate *processes*, not to resist an adversary, so a
//! small deterministic generator seeded from the clock is exactly the right
//! amount of machinery - and it lets a test seed it and get a repeatable
//! sequence, which a real entropy source would not.

use std::time::Duration;

/// Capped exponential backoff with proportional jitter.
///
/// `delay(1)` is `initial`; each subsequent attempt multiplies by `multiplier`
/// until `max`, and then the result is jittered downward-or-upward by up to
/// `jitter_percent` of itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Backoff {
    initial_ms: u64,
    max_ms: u64,
    multiplier: u32,
    jitter_percent: u32,
}

/// A configuration that would produce a retry loop we are not willing to run.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum BackoffError {
    #[error(
        "backoff initial delay must be at least 1ms; a zero delay is a hot loop \
         against the exchange"
    )]
    ZeroInitial,

    #[error("backoff max delay ({max_ms}ms) must be at least the initial delay ({initial_ms}ms)")]
    MaxBelowInitial { max_ms: u64, initial_ms: u64 },

    #[error("backoff multiplier must be at least 1; a smaller one would shrink the delay")]
    ZeroMultiplier,

    #[error("backoff jitter must be a percentage in 0..=100, got {percent}")]
    JitterOutOfRange { percent: u32 },
}

impl Backoff {
    /// Build a backoff schedule.
    ///
    /// # Errors
    ///
    /// [`BackoffError`] for any setting that would let the schedule hot-loop or
    /// shrink. There is no clamping: a nonsensical configuration is a refusal to
    /// start, not something quietly corrected into a different policy than the
    /// operator wrote down.
    pub fn new(
        initial: Duration,
        max: Duration,
        multiplier: u32,
        jitter_percent: u32,
    ) -> Result<Self, BackoffError> {
        let initial_ms = u64::try_from(initial.as_millis()).unwrap_or(u64::MAX);
        let max_ms = u64::try_from(max.as_millis()).unwrap_or(u64::MAX);

        if initial_ms == 0 {
            return Err(BackoffError::ZeroInitial);
        }
        if max_ms < initial_ms {
            return Err(BackoffError::MaxBelowInitial { max_ms, initial_ms });
        }
        if multiplier == 0 {
            return Err(BackoffError::ZeroMultiplier);
        }
        if jitter_percent > 100 {
            return Err(BackoffError::JitterOutOfRange {
                percent: jitter_percent,
            });
        }

        Ok(Self {
            initial_ms,
            max_ms,
            multiplier,
            jitter_percent,
        })
    }

    /// The default schedule: 500ms, doubling to 30s, with +/-20% jitter.
    ///
    /// Fast enough that a one-off blip costs half a second of data, capped low
    /// enough that a long outage still recovers within a tick of most alerting.
    #[must_use]
    pub fn default_schedule() -> Self {
        Self {
            initial_ms: 500,
            max_ms: 30_000,
            multiplier: 2,
            jitter_percent: 20,
        }
    }

    /// The un-jittered delay before `attempt`, counting from one.
    ///
    /// `attempt` 0 is treated as 1: there is no such thing as a zero-wait retry
    /// here, because that is the hot loop this type exists to prevent.
    #[must_use]
    pub fn base_delay(self, attempt: u32) -> Duration {
        let steps = attempt.saturating_sub(1);
        let mut ms = self.initial_ms;
        for _ in 0..steps {
            if ms >= self.max_ms {
                break;
            }
            ms = ms.saturating_mul(u64::from(self.multiplier));
        }
        Duration::from_millis(ms.min(self.max_ms))
    }

    /// The delay before `attempt`, with jitter drawn from `rng`.
    ///
    /// The result is within `+/- jitter_percent` of [`Backoff::base_delay`] and
    /// is never zero: a jittered delay that rounded to nothing would be the very
    /// hot loop the cap is there to prevent.
    #[must_use]
    pub fn delay(self, attempt: u32, rng: &mut Jitter) -> Duration {
        let base_ms = u64::try_from(self.base_delay(attempt).as_millis()).unwrap_or(u64::MAX);
        if self.jitter_percent == 0 {
            return Duration::from_millis(base_ms);
        }

        // The full jitter band, in milliseconds: `2 * percent` of the base, of
        // which the lower half is subtracted. Integer throughout - a percentage
        // of a whole number of milliseconds is a whole number of milliseconds.
        let span = base_ms
            .saturating_mul(u64::from(self.jitter_percent))
            .saturating_mul(2)
            / 100;
        let offset = rng.below(span.saturating_add(1));
        let low = base_ms.saturating_sub(span / 2);

        Duration::from_millis(low.saturating_add(offset).max(1))
    }

    #[must_use]
    pub fn initial(self) -> Duration {
        Duration::from_millis(self.initial_ms)
    }

    #[must_use]
    pub fn max(self) -> Duration {
        Duration::from_millis(self.max_ms)
    }

    #[must_use]
    pub fn jitter_percent(self) -> u32 {
        self.jitter_percent
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::default_schedule()
    }
}

/// A small xorshift64* generator, used only to spread reconnect attempts out.
///
/// Not cryptographic, and it does not need to be: the requirement is that two
/// processes dropped by the same `serverShutdown` do not retry in lockstep. A
/// seeded generator also means a test gets the same sequence every run, which is
/// worth more here than unpredictability is.
#[derive(Clone, Copy, Debug)]
pub struct Jitter {
    state: u64,
}

impl Jitter {
    /// Seed explicitly. Used by tests, so a jittered schedule is reproducible.
    #[must_use]
    pub fn from_seed(seed: u64) -> Self {
        // Zero is the one state xorshift cannot leave, so it is mapped away.
        Self {
            state: if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            },
        }
    }

    /// Seed from the wall clock, so separate processes diverge.
    ///
    /// This is the only clock read in the reconnect path, and it feeds nothing
    /// but the jitter - no recorded value, no timestamp, and nothing a replay
    /// reproduces - so it does not compromise determinism where that matters.
    #[must_use]
    pub fn from_entropy() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
        Self::from_seed(nanos ^ u64::from(std::process::id()).rotate_left(32))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A value in `0..bound`, or 0 when `bound` is 0.
    fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }
}

impl Default for Jitter {
    fn default() -> Self {
        Self::from_entropy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backoff() -> Backoff {
        Backoff::new(
            Duration::from_millis(100),
            Duration::from_millis(1_600),
            2,
            0,
        )
        .expect("a valid schedule")
    }

    #[test]
    fn the_delay_grows_geometrically_and_then_stops_at_the_cap() {
        let b = backoff();
        let ms = |attempt| b.base_delay(attempt).as_millis();
        assert_eq!(
            (1..=8).map(ms).collect::<Vec<_>>(),
            vec![100, 200, 400, 800, 1_600, 1_600, 1_600, 1_600]
        );
    }

    #[test]
    fn the_delay_is_never_zero_at_any_attempt() {
        // The property that matters more than the exact curve: no configuration
        // and no attempt number produces a retry with no wait in front of it.
        let b = Backoff::default_schedule();
        let mut rng = Jitter::from_seed(1);
        for attempt in [0, 1, 2, 5, 50, u32::MAX] {
            assert!(
                b.base_delay(attempt) >= Duration::from_millis(1),
                "attempt {attempt} would hot-loop"
            );
            assert!(b.delay(attempt, &mut rng) >= Duration::from_millis(1));
        }
    }

    #[test]
    fn an_absurd_attempt_number_saturates_at_the_cap_rather_than_wrapping() {
        let b = Backoff::default_schedule();
        assert_eq!(b.base_delay(u32::MAX), b.max());
        assert_eq!(b.base_delay(1_000), b.max());
    }

    #[test]
    fn jitter_stays_inside_its_band_and_actually_varies() {
        let b = Backoff::new(
            Duration::from_millis(1_000),
            Duration::from_millis(1_000),
            2,
            20,
        )
        .expect("a valid schedule");
        let mut rng = Jitter::from_seed(0xDEAD_BEEF);

        let samples: Vec<u128> = (0..500).map(|_| b.delay(3, &mut rng).as_millis()).collect();

        for ms in &samples {
            assert!(
                (800..=1_200).contains(ms),
                "{ms}ms is outside +/-20% of 1000ms"
            );
        }
        let distinct: std::collections::BTreeSet<_> = samples.iter().collect();
        assert!(
            distinct.len() > 100,
            "jitter that barely varies does not decorrelate anything: {} distinct values",
            distinct.len()
        );
    }

    #[test]
    fn zero_jitter_returns_the_base_delay_exactly() {
        let b = backoff();
        let mut rng = Jitter::from_seed(7);
        for attempt in 1..=6 {
            assert_eq!(b.delay(attempt, &mut rng), b.base_delay(attempt));
        }
    }

    #[test]
    fn a_seeded_jitter_is_reproducible() {
        let b = Backoff::default_schedule();
        let run = || {
            let mut rng = Jitter::from_seed(99);
            (1..=10).map(|a| b.delay(a, &mut rng)).collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn two_entropy_seeded_jitters_do_not_march_in_lockstep() {
        // The entire point of jitter: two processes dropped by the same event
        // must not retry at the same instants.
        let b = Backoff::default_schedule();
        let schedule = |mut rng: Jitter| (1..=10).map(|a| b.delay(a, &mut rng)).collect::<Vec<_>>();
        assert_ne!(
            schedule(Jitter::from_entropy()),
            schedule(Jitter::from_entropy())
        );
    }

    #[test]
    fn a_schedule_that_could_hot_loop_or_shrink_refuses_to_build() {
        let ms = Duration::from_millis;
        assert_eq!(
            Backoff::new(ms(0), ms(100), 2, 0),
            Err(BackoffError::ZeroInitial)
        );
        assert_eq!(
            Backoff::new(ms(100), ms(50), 2, 0),
            Err(BackoffError::MaxBelowInitial {
                max_ms: 50,
                initial_ms: 100
            })
        );
        assert_eq!(
            Backoff::new(ms(100), ms(1_000), 0, 0),
            Err(BackoffError::ZeroMultiplier)
        );
        assert_eq!(
            Backoff::new(ms(100), ms(1_000), 2, 101),
            Err(BackoffError::JitterOutOfRange { percent: 101 })
        );
    }

    #[test]
    fn a_multiplier_of_one_is_a_constant_schedule_not_a_shrinking_one() {
        let b = Backoff::new(Duration::from_millis(250), Duration::from_secs(30), 1, 0)
            .expect("a valid schedule");
        for attempt in 1..=10 {
            assert_eq!(b.base_delay(attempt), Duration::from_millis(250));
        }
    }
}
