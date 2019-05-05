//! Implementation of Hybrid Logical Clocks.
//!
//! This is based on the paper "Logical Physical Clocks and Consistent
//! Snapshots in Globally Distributed Databases". Provides a
//! strictly-monotonic clock that can be used to determine if one event
//! `happens-before` another.

#![deny(warnings)]

extern crate time;

#[macro_use]
extern crate quick_error;

#[cfg(feature = "serde")]
extern crate serde;
#[cfg(all(feature = "serde", test))]
extern crate serde_json;
#[cfg(test)]
extern crate suppositions;

use std::cell::Cell;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt;
use std::io;
use std::ops::Sub;

use time::Duration;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        OffsetTooGreat {
        }
    }
}

/// Describes the interface that the inner clock source must provide.
pub trait ClockSource {
    /// Represents the described clock time.
    type Time: Ord + Copy + Sub<Output = Self::Delta> + fmt::Debug;
    /// The difference between two timestamps.
    type Delta: Ord;
    /// Returns the current clock time.
    fn now(&mut self) -> Self::Time;
}

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
    /// The wall-clock time as returned by the clock source.
    pub time: T,
    /// A Lamport clock used to disambiguate events that are given the same
    /// wall-clock time. This is reset whenever `time` is incremented.
    pub count: u32,
}

/// A clock source that returns wall-clock in nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Wall;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Nanoseconds since unix epoch
pub struct WallT(u64);

/// The main clock type.
#[derive(Debug, Clone)]
pub struct Clock<S: ClockSource> {
    src: S,
    epoch: u32,
    last_observed: Timestamp<S::Time>,
    max_offset: Option<S::Delta>,
}

impl Clock<Wall> {
    /// Returns a `Clock` that uses wall-clock time.
    pub fn wall() -> Clock<Wall> {
        Clock::new(Wall)
    }
}

impl Clock<ManualClock> {
    /// Returns a `Clock` that uses wall-clock time.
    pub fn manual(t: u64) -> Clock<ManualClock> {
        Clock::new(ManualClock::new(t))
    }
    pub fn set_time(&mut self, t: u64) {
        self.src.set_time(t)
    }
}

impl<S: ClockSource> Clock<S> {
    /// Creates a clock with `src` as the time provider.
    pub fn new(mut src: S) -> Self {
        let init = src.now();
        Clock {
            src: src,
            last_observed: Timestamp {
                epoch: 0,
                time: init,
                count: 0,
            },
            max_offset: None,
            epoch: 0,
        }
    }

    /// Creates a clock with `src` as the time provider, and `diff` as how far
    /// in the future we don't mind seeing updates from.
    pub fn new_with_max_diff(mut src: S, diff: S::Delta) -> Self {
        let init = src.now();
        Clock {
            src: src,
            last_observed: Timestamp {
                epoch: 0,
                time: init,
                count: 0,
            },
            max_offset: Some(diff),
            epoch: 0,
        }
    }

    /// Used to create a new "epoch" of clock times, mostly useful as a manual
    /// override when a cluster member has skewed the clock time far
    /// into the future.
    pub fn set_epoch(&mut self, epoch: u32) {
        self.epoch = epoch;
    }

    /// Creates a unique monotonic timestamp suitable for annotating messages we send.
    pub fn now(&mut self) -> Timestamp<S::Time> {
        let pt = self.read_pt();
        self.do_observe(&pt);
        self.last_observed
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
    /// input message. Returns an Error iff the delta from our local lock to
    /// the observed timestamp is greater than our configured limit.
    pub fn observe(&mut self, msg: &Timestamp<S::Time>) -> Result<(), Error> {
        let pt = self.read_pt();
        try!(self.verify_offset(&pt, msg));
        self.do_observe(&msg);
        Ok(())
    }

    fn read_pt(&mut self) -> Timestamp<S::Time> {
        Timestamp {
            epoch: self.epoch,
            time: self.src.now(),
            count: 0,
        }
    }

    fn verify_offset(
        &self,
        pt: &Timestamp<S::Time>,
        msg: &Timestamp<S::Time>,
    ) -> Result<(), Error> {
        if let Some(ref max) = self.max_offset {
            let diff = msg.time - pt.time;
            if &diff > max {
                return Err(Error::OffsetTooGreat);
            }
        }

        Ok(())
    }
}

impl Timestamp<WallT> {
    pub fn write_bytes<W: io::Write>(&self, mut wr: W) -> Result<(), io::Error> {
        wr.write_all(&self.to_bytes())?;
        return Ok(());
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        let mut res = [0; 16];
        res[0..4].copy_from_slice(&self.epoch.to_be_bytes());
        res[4..12].copy_from_slice(&self.time.0.to_be_bytes());
        res[12..16].copy_from_slice(&self.count.to_be_bytes());
        return res;
    }

    pub fn read_bytes<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let mut buf = [0u8; 16];
        r.read_exact(&mut buf)?;
        Ok(Self::from_bytes(buf))
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let epoch = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let nanos = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
        let count = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        Timestamp {
            epoch: epoch,
            time: WallT(nanos),
            count: count,
        }
    }
}

const NANOS_PER_SEC: u64 = 1000_000_000;

impl WallT {
    /// Returns a `time::Timespec` representing this timestamp.
    pub fn as_timespec(self) -> time::Timespec {
        let secs = self.0 / NANOS_PER_SEC;
        let nsecs = self.0 % NANOS_PER_SEC;
        time::Timespec {
            sec: secs as i64,
            nsec: nsecs as i32,
        }
    }

    /// Returns a `WallT` representing the `time::Timespec`.
    fn from_timespec(t: time::Timespec) -> Self {
        WallT(t.sec as u64 * NANOS_PER_SEC + t.nsec as u64)
    }

    /// Returns time in nanoseconds since the unix epoch.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl Sub for WallT {
    type Output = Duration;
    fn sub(self, rhs: Self) -> Self::Output {
        let nanos = self.0 - rhs.0;
        Duration::nanoseconds(nanos as i64)
    }
}

impl ClockSource for Wall {
    type Time = WallT;
    type Delta = Duration;
    fn now(&mut self) -> Self::Time {
        WallT::from_timespec(time::get_time())
    }
}

impl fmt::Display for WallT {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let tm = time::at_utc(self.as_timespec());
        write!(
            fmt,
            "{}",
            tm.strftime("%Y-%m-%dT%H:%M:%S.%fZ").expect("strftime")
        )
    }
}

impl<T: fmt::Display> fmt::Display for Timestamp<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}:{}+{}", self.epoch, self.time, self.count)
    }
}

pub struct ManualClock(Cell<u64>);

impl<'a> ClockSource for ManualClock {
    type Time = u64;
    type Delta = u64;
    fn now(&mut self) -> Self::Time {
        self.0.get()
    }
}

impl ManualClock {
    pub fn new(t: u64) -> ManualClock {
        ManualClock(Cell::new(t))
    }
    pub fn set_time(&self, t: u64) {
        self.0.set(t)
    }
}

#[cfg(feature = "serde")]
mod serde_impl;

#[cfg(test)]
mod tests {
    use super::{Clock, ManualClock, Timestamp, WallT};
    use std::io::Cursor;
    use suppositions::generators::*;
    use suppositions::*;

    fn observing<'a>(
        clock: &mut Clock<ManualClock>,
        msg: &Timestamp<u64>,
    ) -> Result<Timestamp<u64>, super::Error> {
        try!(clock.observe(msg));
        Ok(clock.now())
    }

    fn wallclocks() -> Box<GeneratorObject<Item = WallT>> {
        u64s().map(WallT).boxed()
    }

    fn timestamps<C: Generator + 'static>(
        times: C,
    ) -> Box<GeneratorObject<Item = Timestamp<C::Item>>> {
        let epochs = u32s();
        let counts = u32s();
        (epochs, times, counts)
            .map(|(epoch, time, count)| Timestamp { epoch, time, count })
            .boxed()
    }

    #[test]
    fn fig_6_proc_0_a() {
        let mut clock = Clock::manual(0);
        clock.set_time(10);
        assert_eq!(
            clock.now(),
            Timestamp {
                epoch: 0,
                time: 10,
                count: 0
            }
        )
    }

    #[test]
    fn fig_6_proc_1_a() {
        let mut clock = Clock::manual(1);
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
        )
    }

    #[test]
    fn fig_6_proc_1_b() {
        let mut clock = Clock::manual(1);
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
            clock.now(),
            Timestamp {
                epoch: 0,
                time: 10,
                count: 2
            }
        )
    }

    #[test]
    fn fig_6_proc_2_b() {
        let mut clock = Clock::manual(0);
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
        )
    }

    #[test]
    fn fig_6_proc_2_c() {
        let mut clock = Clock::manual(0);
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
            clock.now(),
            Timestamp {
                epoch: 0,
                time: 10,
                count: 4
            }
        )
    }

    #[test]
    fn all_sources_same() {
        let mut clock = Clock::manual(0);
        let observed = Timestamp {
            epoch: 0,
            time: 0,
            count: 5,
        };
        let result = observing(&mut clock, &observed).unwrap();
        println!("obs:{:?}; result:{:?}", observed, result);
        assert!(result > observed);
        assert!(result.time == observed.time)
    }

    #[test]
    fn handles_time_going_backwards_now() {
        let mut clock = Clock::manual(10);
        let _ = clock.now();
        clock.set_time(9);
        assert_eq!(
            clock.now(),
            Timestamp {
                epoch: 0,
                time: 10,
                count: 2
            }
        )
    }

    #[test]
    fn handles_time_going_backwards_observe() {
        let mut clock = Clock::manual(10);
        let original = clock.now();
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
    }

    #[test]
    fn handles_time_going_forwards_now() {
        let mut clock = Clock::manual(10);
        let t = clock.now();
        println!("at 10: {}", t);
        clock.set_time(12);
        let t2 = clock.now();
        println!("=> 12: {}", t2);
        assert_eq!(
            t2,
            Timestamp {
                epoch: 0,
                time: 12,
                count: 0
            }
        )
    }

    #[test]
    fn handles_time_going_forwards_observe() {
        let mut clock = Clock::manual(10);
        let _ = clock.now();
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
        )
    }

    #[test]
    fn should_order_primarily_via_epoch() {
        let mut clock0 = Clock::manual(10);
        clock0.set_epoch(0);
        let mut clock1 = Clock::manual(0);
        clock1.set_epoch(1);

        let a = clock0.now();
        let b = clock1.now();
        println!("a: {} < b: {}", a, b);
        assert!(a < b);
    }

    #[test]
    fn should_apply_configured_epoch() {
        let mut clock0 = Clock::manual(10);

        let _ = clock0.now();

        clock0.set_epoch(1);

        clock0.set_time(1);

        let a = clock0.now();

        assert_eq!(
            a,
            Timestamp {
                epoch: 1,
                time: 1,
                count: 0
            }
        );
    }

    #[test]
    fn should_update_via_observed_epochs() {
        let mut clock0 = Clock::manual(10);
        clock0.set_epoch(0);

        let _ = clock0.now();

        let mut clock1 = Clock::manual(0);
        clock1.set_epoch(1);

        clock0.set_time(1);
        clock1.set_time(1);

        let a = clock1.now();

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
    }

    #[test]
    fn should_remember_epochs() {
        let mut clock0 = Clock::manual(10);
        clock0.set_epoch(0);

        let mut clock1 = Clock::manual(0);
        clock1.set_epoch(1);

        clock0.set_time(1);
        clock1.set_time(1);

        let a = clock1.now();
        let _ = observing(&mut clock0, &a).unwrap();
        let b = clock0.now();
        println!("a: {}; b:{}", a, b);
        assert_eq!(
            b,
            Timestamp {
                epoch: 1,
                time: 1,
                count: 2
            }
        );
    }

    #[test]
    fn should_use_time_from_larger_observed_epoch() {
        let mut clock0 = Clock::manual(10);

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
    }

    #[test]
    fn supposedly_be_larger_than_observed_time() {
        property((u64s(), timestamps(u64s()))).check(|(t0, advanced_epoch)| {
            let mut clock0 = Clock::manual(t0);
            let t2 = observing(&mut clock0, &advanced_epoch).unwrap();
            println!("t0: {:?}; 👀: {:?} => {:?}", t0, advanced_epoch, t2);
            assert!(t2 > advanced_epoch, "{:?} > {:?}", t2, advanced_epoch)
        });
    }

    #[test]
    fn supposedly_be_larger_than_observed_clock() {
        property((u64s(), timestamps(u64s()))).check(|(t0, advanced_epoch)| {
            let mut clock0 = Clock::manual(t0);
            let t1 = clock0.now();
            let t2 = observing(&mut clock0, &advanced_epoch).unwrap();
            println!("t0: {:?}; 👀: {:?} => {:?}", t0, advanced_epoch, t2);
            assert!(t2 > t1, "{:?} > {:?}", t2, t1)
        });
    }

    #[test]
    fn should_ignore_clocks_too_far_forward() {
        let src = ManualClock::new(0);
        let mut clock = Clock::new_with_max_diff(src, 10);
        assert!(observing(
            &mut clock,
            &Timestamp {
                epoch: 0,
                time: 11,
                count: 0
            }
        )
        .is_err());
        assert_eq!(
            observing(
                &mut clock,
                &Timestamp {
                    epoch: 0,
                    time: 1,
                    count: 0
                }
            )
            .unwrap(),
            Timestamp {
                epoch: 0,
                time: 1,
                count: 1
            }
        )
    }

    #[test]
    fn should_account_for_time_passing_when_checking_max_error() {
        let src = ManualClock::new(0);
        let mut clock = Clock::new_with_max_diff(src, 10);
        clock.set_time(1);
        assert!(observing(
            &mut clock,
            &Timestamp {
                epoch: 0,
                time: 11,
                count: 0
            }
        )
        .is_ok());
    }

    #[test]
    fn should_round_trip_via_key() {
        property(timestamps(wallclocks())).check(|ts| {
            let mut bs = Vec::new();
            ts.write_bytes(&mut bs).expect("write_bytes");
            let ts2 = Timestamp::read_bytes(Cursor::new(&bs)).expect("read_bytes");
            // println!("{:?}\t{:?}", ts == ts2, bs);
            ts == ts2
        });
    }

    #[test]
    fn byte_repr_should_order_as_timestamps() {
        property((timestamps(wallclocks()), timestamps(wallclocks()))).check(|(ta, tb)| {
            use std::cmp::Ord;

            let mut ba = Vec::new();
            let mut bb = Vec::new();
            ta.write_bytes(&mut ba).expect("write_bytes");
            tb.write_bytes(&mut bb).expect("write_bytes");
            /*
            println!("{:?}\t{:?} <> {:?}: {:?}\t{:?} <> {:?}: {:?}",
                    ta.cmp(&tb) == ba.cmp(&bb),
                    ta, tb, ta.cmp(&tb),
                    ba, bb, ba.cmp(&bb));
            */
            ta.cmp(&tb) == ba.cmp(&bb)
        })
    }

    #[cfg(feature = "serde")]
    mod serde {
        use super::*;
        use serde_json;
        use suppositions::*;
        #[test]
        fn should_round_trip_via_serde() {
            property(timestamps(u64s())).check(|ts| {
                let s = serde_json::to_string(&ts).expect("to-json");
                let ts2 = serde_json::from_str(&s).expect("from-json");
                ts == ts2
            });
        }
    }
}
