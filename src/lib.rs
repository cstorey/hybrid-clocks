//! Implementation of Hybrid Logical Clocks.
//!
//! This is based on the paper "Logical Physical Clocks and Consistent
//! Snapshots in Globally Distributed Databases". Provides a
//! strictly-monotonic clock that can be used to determine if one event
//! `happens-before` another.

#![deny(warnings)]

#[cfg(feature = "serialization")]
extern crate serde;
#[cfg(feature = "serialization")]
#[macro_use]
extern crate serde_derive;
#[cfg(all(feature = "serialization", test))]
extern crate serde_json;

use std::cmp::Ordering;
use std::fmt;

use thiserror::Error;

mod source;
pub use crate::source::*;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Offset greater than limit")]
    OffsetTooGreat,
    #[error("Outside of specified offset")]
    SystemTime(#[from] std::time::SystemTimeError),
    #[error("Integer conversion error")]
    FromInt(#[from] std::num::TryFromIntError),
    #[error("Outside supported time range: {0}ticks")]
    SupportedTime(u128),
}

pub type Result<T> = std::result::Result<T, Error>;

/// A value that represents a logical timestamp.
///
/// These allow us to describe at least a partial ordering over events, in the
/// same style as Lamport Clocks. In summary, if `a < b` then we can say that `a` logically
/// `happens-before` `b`. Because they are scalar values, they can't be used to tell between whether:
///
///  * `a` happenned concurrently with `b`, or
///  * `a` is part of `b`'s causal history, or vica-versa.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp<T> {
    /// An epoch counter.
    pub epoch: u32,
    /// The Wall-clock time as returned by the clock source.
    pub time: T,
    /// A Lamport clock used to disambiguate events that are given the same
    /// Wall-clock time. This is reset whenever `time` is incremented.
    pub count: u32,
}

/// The main clock type.
#[derive(Debug, Clone)]
pub struct Clock<S: ClockSource> {
    src: S,
    epoch: u32,
    last_observed: Timestamp<S::Time>,
}

/// A wrapper around `Clock` that will refuse updates outside of our tolerance.
#[derive(Debug, Clone)]
pub struct OffsetLimiter<S: ClockSource> {
    clock: Clock<S>,
    max_offset: S::Delta,
}

impl Clock<WallNS> {
    /// Returns a `Clock` that uses WallNS-clock time.
    pub fn wall_ns() -> Result<Clock<WallNS>> {
        Clock::new(WallNS)
    }
}

impl Clock<WallMS> {
    /// Returns a `Clock` that uses WallNS-clock time.
    pub fn wall_ms() -> Result<Clock<WallMS>> {
        Clock::new(WallMS)
    }
}

impl Clock<ManualClock> {
    /// Returns a `Clock` that uses WallNS-clock time.
    pub fn manual(t: u64) -> Result<Clock<ManualClock>> {
        Clock::new(ManualClock::new(t))
    }
    pub fn set_time(&mut self, t: u64) {
        self.src.set_time(t)
    }
}

impl<S: ClockSource> Clock<S> {
    /// Creates a clock with `src` as the time provider.
    pub fn new(mut src: S) -> Result<Self> {
        let init = src.now()?;
        let clock = Clock {
            src: src,
            last_observed: Timestamp {
                epoch: 0,
                time: init,
                count: 0,
            },
            epoch: 0,
        };
        Ok(clock)
    }

    /// Creates a clock with `src` as the time provider, and `diff` as how far
    /// in the future we don't mind seeing updates from.
    pub fn with_max_diff(self, max_offset: S::Delta) -> OffsetLimiter<S> {
        OffsetLimiter {
            clock: self,
            max_offset,
        }
    }

    /// Used to create a new "epoch" of clock times, mostly useful as a manual
    /// override when a cluster member has skewed the clock time far
    /// into the future.
    pub fn set_epoch(&mut self, epoch: u32) {
        self.epoch = epoch;
    }

    /// Creates a unique monotonic timestamp suitable for annotating messages we send.
    pub fn now(&mut self) -> Result<Timestamp<S::Time>> {
        let pt = self.read_pt()?;
        self.do_observe(&pt);
        Ok(self.last_observed)
    }

    fn do_observe(&mut self, observation: &Timestamp<S::Time>) {
        let lp = self.last_observed.clone();

        self.last_observed = match (
            lp.epoch.cmp(&observation.epoch),
            lp.time.cmp(&observation.time),
            lp.count.cmp(&observation.count),
        ) {
            (Ordering::Less, _, _) | (Ordering::Equal, Ordering::Less, _) => observation.clone(),
            (Ordering::Equal, Ordering::Equal, Ordering::Less) => Timestamp {
                count: observation.count + 1,
                ..lp
            },
            _ => Timestamp {
                count: lp.count + 1,
                ..lp
            },
        };
    }

    /// Accepts a timestamp from an incoming message, and updates the clock
    /// so that further calls to `now` will always return a timestamp that
    /// `happens-after` either locally generated timestamps or that of the
    /// input message.
    pub fn observe(&mut self, msg: &Timestamp<S::Time>) {
        self.do_observe(&msg);
    }

    fn read_pt(&mut self) -> Result<Timestamp<S::Time>> {
        Ok(Timestamp {
            epoch: self.epoch,
            time: self.src.now()?,
            count: 0,
        })
    }
}
impl<S: ClockSource> OffsetLimiter<S> {
    /// Accepts a timestamp from an incoming message, and updates the clock
    /// so that further calls to `now` will always return a timestamp that
    /// `happens-after` either locally generated timestamps or that of the
    /// input message. Returns an Error iff the delta from our local lock to
    /// the observed timestamp is greater than our configured limit.
    pub fn observe(&mut self, msg: &Timestamp<S::Time>) -> Result<()> {
        let pt = self.clock.read_pt()?;
        self.verify_offset(&pt, msg)?;
        self.clock.observe(&msg);
        Ok(())
    }

    /// Creates a unique monotonic timestamp suitable for annotating messages we send.
    pub fn now(&mut self) -> Result<Timestamp<S::Time>> {
        self.clock.now()
    }

    fn verify_offset(&self, pt: &Timestamp<S::Time>, msg: &Timestamp<S::Time>) -> Result<()> {
        let diff = msg.time - pt.time;
        if diff > self.max_offset {
            return Err(Error::OffsetTooGreat);
        }

        Ok(())
    }

    /// Extract the inner `Clock`
    pub fn into_inner(self) -> Clock<S> {
        self.clock
    }

    /// Get a reference to the inner `Clock`
    pub fn inner(&self) -> &Clock<S> {
        &self.clock
    }

    /// Get a mutable reference to the inner `Clock`
    pub fn inner_mut(&mut self) -> &mut Clock<S> {
        &mut self.clock
    }
}

impl<T: fmt::Display> fmt::Display for Timestamp<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{}:{}+{}", self.epoch, self.time, self.count)
    }
}

impl<T> Timestamp<T> {
    pub fn time_into<U: From<T>>(self) -> Timestamp<U> {
        Timestamp {
            epoch: self.epoch,
            time: self.time.into(),
            count: self.count,
        }
    }
}

#[cfg(feature = "serialization")]
mod serde_impl;

#[cfg(test)]
mod tests {
    // TODO: Use anyhow::Error for backtraces
    use super::{Clock, ManualClock, Result, Timestamp};
    use suppositions::generators::*;
    use suppositions::*;

    fn observing<'a>(
        clock: &mut Clock<ManualClock>,
        msg: &Timestamp<u64>,
    ) -> Result<Timestamp<u64>> {
        clock.observe(msg);
        Ok(clock.now()?)
    }

    pub fn timestamps<C: Generator + 'static>(
        times: C,
    ) -> Box<dyn GeneratorObject<Item = Timestamp<C::Item>>> {
        let epochs = u32s();
        let counts = u32s();
        (epochs, times, counts)
            .map(|(epoch, time, count)| Timestamp { epoch, time, count })
            .boxed()
    }

    #[test]
    fn fig_6_proc_0_a() -> Result<()> {
        let mut clock = Clock::manual(0)?;
        clock.set_time(10);
        assert_eq!(
            clock.now()?,
            Timestamp {
                epoch: 0,
                time: 10,
                count: 0
            }
        );
        Ok(())
    }

    #[test]
    fn fig_6_proc_1_a() -> Result<()> {
        let mut clock = Clock::manual(1)?;
        assert_eq!(
            observing(
                &mut clock,
                &Timestamp {
                    epoch: 0,
                    time: 10,
                    count: 0
                }
            )
            .unwrap(),
            Timestamp {
                epoch: 0,
                time: 10,
                count: 1
            }
        );
        Ok(())
    }

    #[test]
    fn fig_6_proc_1_b() -> Result<()> {
        let mut clock = Clock::manual(1)?;
        let _ = observing(
            &mut clock,
            &Timestamp {
                epoch: 0,
                time: 10,
                count: 0,
            },
        )
        .unwrap();
        clock.set_time(2);
        assert_eq!(
            clock.now()?,
            Timestamp {
                epoch: 0,
                time: 10,
                count: 2
            }
        );
        Ok(())
    }

    #[test]
    fn fig_6_proc_2_b() -> Result<()> {
        let mut clock = Clock::manual(0)?;
        clock.last_observed = Timestamp {
            epoch: 0,
            time: 1,
            count: 0,
        };
        clock.set_time(2);
        assert_eq!(
            observing(
                &mut clock,
                &Timestamp {
                    epoch: 0,
                    time: 10,
                    count: 2
                }
            )
            .unwrap(),
            Timestamp {
                epoch: 0,
                time: 10,
                count: 3
            }
        );
        Ok(())
    }

    #[test]
    fn fig_6_proc_2_c() -> Result<()> {
        let mut clock = Clock::manual(0)?;
        clock.set_time(2);
        let _ = observing(
            &mut clock,
            &Timestamp {
                epoch: 0,
                time: 10,
                count: 2,
            },
        )
        .unwrap();
        clock.set_time(3);
        assert_eq!(
            clock.now()?,
            Timestamp {
                epoch: 0,
                time: 10,
                count: 4
            }
        );
        Ok(())
    }

    #[test]
    fn all_sources_same() -> Result<()> {
        let mut clock = Clock::manual(0)?;
        let observed = Timestamp {
            epoch: 0,
            time: 0,
            count: 5,
        };
        let result = observing(&mut clock, &observed)?;
        println!("obs:{:?}; result:{:?}", observed, result);
        assert!(result > observed);
        assert!(result.time == observed.time);
        Ok(())
    }

    #[test]
    fn handles_time_going_backwards_now() -> Result<()> {
        let mut clock = Clock::manual(10)?;
        let _ = clock.now()?;
        clock.set_time(9);
        assert_eq!(
            clock.now()?,
            Timestamp {
                epoch: 0,
                time: 10,
                count: 2
            }
        );
        Ok(())
    }

    #[test]
    fn handles_time_going_backwards_observe() -> Result<()> {
        let mut clock = Clock::manual(10)?;
        let original = clock.now()?;
        clock.set_time(9);
        let result = observing(
            &mut clock,
            &Timestamp {
                epoch: 0,
                time: 0,
                count: 0,
            },
        )
        .unwrap();
        assert!(result > original);
        assert!(result.time == 10);
        Ok(())
    }

    #[test]
    fn handles_time_going_forwards_now() -> Result<()> {
        let mut clock = Clock::manual(10)?;
        let t = clock.now()?;
        println!("at 10: {}", t);
        clock.set_time(12);
        let t2 = clock.now()?;
        println!("=> 12: {}", t2);
        assert_eq!(
            t2,
            Timestamp {
                epoch: 0,
                time: 12,
                count: 0
            }
        );
        Ok(())
    }

    #[test]
    fn handles_time_going_forwards_observe() -> Result<()> {
        let mut clock = Clock::manual(10)?;
        let _ = clock.now()?;
        clock.set_time(12);
        assert_eq!(
            observing(
                &mut clock,
                &Timestamp {
                    epoch: 0,
                    time: 0,
                    count: 0
                }
            )
            .unwrap(),
            Timestamp {
                epoch: 0,
                time: 12,
                count: 0
            }
        );
        Ok(())
    }

    #[test]
    fn should_order_primarily_via_epoch() -> Result<()> {
        let mut clock0 = Clock::manual(10)?;
        clock0.set_epoch(0);
        let mut clock1 = Clock::manual(0)?;
        clock1.set_epoch(1);

        let a = clock0.now()?;
        let b = clock1.now()?;
        println!("a: {} < b: {}", a, b);
        assert!(a < b);
        Ok(())
    }

    #[test]
    fn should_apply_configured_epoch() -> Result<()> {
        let mut clock0 = Clock::manual(10)?;

        let _ = clock0.now();

        clock0.set_epoch(1);

        clock0.set_time(1);

        let a = clock0.now()?;

        assert_eq!(
            a,
            Timestamp {
                epoch: 1,
                time: 1,
                count: 0
            }
        );
        Ok(())
    }

    #[test]
    fn should_update_via_observed_epochs() -> Result<()> {
        let mut clock0 = Clock::manual(10)?;
        clock0.set_epoch(0);

        let _ = clock0.now();

        let mut clock1 = Clock::manual(0)?;
        clock1.set_epoch(1);

        clock0.set_time(1);
        clock1.set_time(1);

        let a = clock1.now()?;

        let b = observing(&mut clock0, &a).unwrap();
        println!("a: {}; b: {}", a, b);
        assert_eq!(
            a,
            Timestamp {
                epoch: 1,
                time: 1,
                count: 0
            }
        );
        assert_eq!(
            b,
            Timestamp {
                epoch: 1,
                time: 1,
                count: 1
            }
        );
        Ok(())
    }

    #[test]
    fn should_remember_epochs() -> Result<()> {
        let mut clock0 = Clock::manual(10)?;
        clock0.set_epoch(0);

        let mut clock1 = Clock::manual(0)?;
        clock1.set_epoch(1);

        clock0.set_time(1);
        clock1.set_time(1);

        let a = clock1.now()?;
        let _ = observing(&mut clock0, &a).unwrap();
        let b = clock0.now()?;
        println!("a: {}; b:{}", a, b);
        assert_eq!(
            b,
            Timestamp {
                epoch: 1,
                time: 1,
                count: 2
            }
        );
        Ok(())
    }

    #[test]
    fn should_use_time_from_larger_observed_epoch() -> Result<()> {
        let mut clock0 = Clock::manual(10)?;

        let advanced_epoch = Timestamp {
            epoch: 100,
            time: 1,
            count: 0,
        };
        let t = observing(&mut clock0, &advanced_epoch).unwrap();
        assert_eq!(
            t,
            Timestamp {
                epoch: 100,
                time: 1,
                count: 1
            }
        );
        Ok(())
    }

    #[test]
    fn supposedly_be_larger_than_observed_time() -> Result<()> {
        property((u64s(), timestamps(u64s()))).check(|(t0, advanced_epoch)| -> Result<()> {
            let mut clock0 = Clock::manual(t0)?;
            let t2 = observing(&mut clock0, &advanced_epoch).unwrap();
            println!("t0: {:?}; 👀: {:?} => {:?}", t0, advanced_epoch, t2);
            assert!(t2 > advanced_epoch, "{:?} > {:?}", t2, advanced_epoch);
            Ok(())
        });
        Ok(())
    }

    #[test]
    fn supposedly_be_larger_than_observed_clock() -> Result<()> {
        property((u64s(), timestamps(u64s()))).check(|(t0, advanced_epoch)| -> Result<()> {
            let mut clock0 = Clock::manual(t0)?;
            let t1 = clock0.now()?;
            let t2 = observing(&mut clock0, &advanced_epoch).unwrap();
            println!("t0: {:?}; 👀: {:?} => {:?}", t0, advanced_epoch, t2);
            assert!(t2 > t1, "{:?} > {:?}", t2, t1);
            Ok(())
        });
        Ok(())
    }

    #[test]
    fn should_ignore_clocks_too_far_forward() -> Result<()> {
        let src = ManualClock::new(0);
        let mut clock = Clock::new(src)?.with_max_diff(10);
        assert!(clock
            .observe(&Timestamp {
                epoch: 0,
                time: 11,
                count: 0
            })
            .is_err());

        clock
            .observe(&Timestamp {
                epoch: 0,
                time: 1,
                count: 0,
            })
            .unwrap();
        assert_eq!(
            clock.now().expect("now"),
            Timestamp {
                epoch: 0,
                time: 1,
                count: 1
            }
        );
        Ok(())
    }

    #[test]
    fn should_account_for_time_passing_when_checking_max_error() -> Result<()> {
        let src = ManualClock::new(0);
        let mut clock = Clock::new(src)?.with_max_diff(10);
        clock.clock.set_time(1);

        assert!(clock
            .observe(&Timestamp {
                epoch: 0,
                time: 11,
                count: 0
            })
            .is_ok());
        Ok(())
    }

    #[cfg(feature = "serialization")]
    mod serde {
        use super::*;
        use serde_json;
        #[test]
        fn should_round_trip_via_serde() -> Result<()> {
            property(timestamps(u64s())).check(|ts| {
                let s = serde_json::to_string(&ts).expect("to-json");
                let ts2 = serde_json::from_str(&s).expect("from-json");
                ts == ts2
            });
            Ok(())
        }
    }
}
