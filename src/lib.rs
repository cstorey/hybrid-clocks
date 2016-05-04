//! Implementation of Hybrid Logical Clocks.
//!
//! This is based on the paper "Logical Physical Clocks and Consistent
//! Snapshots in Globally Distributed Databases". Provides a
//! strictly-monotonic clock that can be used to determine if one event
//! `happens-before` another.

extern crate time;
extern crate byteorder;
#[cfg(test)]
extern crate quickcheck;

#[macro_use]
extern crate quick_error;

#[cfg(feature = "serde")]
extern crate serde;
#[cfg(all(feature = "serde", test))]
extern crate serde_json;

use std::cmp::{self,Ordering};
use std::fmt;
use std::io;
use std::ops::Sub;
use time::Duration;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

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
    type Time : Ord + Copy + Sub<Output=Self::Delta> + fmt::Debug;
    /// The difference between two timestamps.
    type Delta : Ord;
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
#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord, Hash)]
pub struct Timestamp<T> {
    /// The wall-clock time as returned by the clock source
    pub time: T,
    /// A Lamport clock used to disambiguate events that are given the same
    /// wall-clock time. This is reset whenever `time` is incremented.
    pub count: u32,
}

/// A clock source that returns wall-clock in nanoseconds.
#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord)]
pub struct Wall;
#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord,Hash)]
/// Nanoseconds since unix epoch
pub struct WallT(u64);

/// The main clock type.
pub struct Clock<S: ClockSource> {
    src: S,
    latest: Timestamp<S::Time>,
    max_offset: Option<S::Delta>,
}

impl Clock<Wall> {
    /// Returns a `Clock` that uses wall-clock time.
    pub fn wall() -> Clock<Wall> {
        Clock::new(Wall)
    }
}

impl<S: ClockSource> Clock<S> {
    /// Creates a clock with `src` as the time provider.
    pub fn new(mut src: S) -> Self {
        let init = src.now();
        Clock {
            src: src,
            latest: Timestamp { time: init, count: 0 },
            max_offset: None,
        }
    }

    /// Creates a clock with `src` as the time provider, and `diff` as how far
    /// in the future we don't mind seeing updates from.
    pub fn new_with_max_diff(mut src: S, diff: S::Delta) -> Self {
        let init = src.now();
        Clock {
            src: src,
            latest: Timestamp { time: init, count: 0 },
            max_offset: Some(diff),
        }
    }

    /// Creates a unique monotonic timestamp suitable for annotating messages we send.
    pub fn on_send(&mut self) -> Timestamp<S::Time> {
        let pt = self.src.now();
        let lp = self.latest.clone();
        println!("pt: {:?}; latest: {:?}; cmp:{:?}", pt, lp, lp.time.cmp(&pt));
        self.latest = match (lp.time.cmp(&pt), lp.count) {
            (Ordering::Less, _) => {
                Timestamp { time: pt, count: 0, .. lp }
            },
            (Ordering::Equal, c) => {
                Timestamp { count: c + 1, .. lp }
            },
            (Ordering::Greater, c) => {
                Timestamp { count: c + 1, .. lp }
            },
        };

        self.latest
    }

    /// Accepts a timestamp from an incoming message, and creates a timestamp
    /// that represents when it arrived; guaranteeing that the input
    /// `happens-before` the returned value. Returns an Error iff the delta
    /// from our latest to the observed timestamp is greater than our
    /// configured limit.
    pub fn on_recv(&mut self, msg: &Timestamp<S::Time>) -> Result<Timestamp<S::Time>, Error> {
        let pt = self.src.now();
        let lp = self.latest.clone();

        try!(self.verify_offset(pt, msg));

        self.latest.time = cmp::max(cmp::max(lp.time, msg.time), pt);
        self.latest.count = match (self.latest.time == lp.time, self.latest.time == msg.time) {
            (true, true) => cmp::max(self.latest.count, msg.count) + 1,
            (true, false) => self.latest.count + 1,
            (false, true) => msg.count + 1,
            (false, false) => 0,
        };

        Ok(self.latest.clone())
    }

    fn verify_offset(&self, pt: S::Time, msg: &Timestamp<S::Time>) -> Result<(), Error> {
        if let Some(ref max) = self.max_offset {
            let diff = msg.time - pt;
            if &diff > max {
                return Err(Error::OffsetTooGreat)
            }
        }

        Ok(())
    }
}

impl Timestamp<WallT> {
    pub fn write_bytes<W: io::Write>(&self, mut wr: W) -> Result<(), io::Error> {
        let wall = &self.time;
        try!(wr.write_u64::<BigEndian>(wall.0));
        try!(wr.write_u32::<BigEndian>(self.count));
        Ok(())
    }

    pub fn read_bytes<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        // use ClockSource;
        let nanos = try!(r.read_u64::<BigEndian>());
        let l = try!(r.read_u32::<BigEndian>());
        let wall = WallT(nanos);
        Ok(Timestamp { time: wall, count: l })
    }
}

const NANOS_PER_SEC : u64 = 1000_000_000;

impl WallT {
    /// Returns a `time::Timespec` representing this timestamp.
    pub fn as_timespec(self) -> time::Timespec {
        let secs = self.0 / NANOS_PER_SEC;
        let nsecs = self.0 % NANOS_PER_SEC;
        time::Timespec { sec: secs as i64, nsec: nsecs as i32 }
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
        write!(fmt, "{}", tm.strftime("%Y-%m-%dT%H:%M:%S.%fZ").expect("strftime"))
    }
}

impl<T: fmt::Display> fmt::Display for Timestamp<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}+{}", self.time, self.count)
    }
}

#[cfg(feature = "serde")]
mod serde_impl;

#[cfg(test)]
mod tests {
    use super::{Clock, ClockSource, Timestamp, WallT};
    use std::cell::Cell;
    use std::cmp::{Ord, Ordering};
    use std::io::Cursor;
    use quickcheck::{self,Arbitrary, Gen};
    use time;

    struct ManualClock(Cell<u64>);

    impl<'a> ClockSource for &'a ManualClock {
        type Time = u64;
        type Delta = u64;
        fn now(&mut self) -> Self::Time {
            self.0.get()
        }
    }

    impl Arbitrary for WallT {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            WallT(Arbitrary::arbitrary(g))
        }
        fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            Box::new(self.0.shrink()
                    .map(WallT))
        }
    }

    impl<T: Arbitrary + Copy> Arbitrary for Timestamp<T> {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let w = Arbitrary::arbitrary(g);
            let l = Arbitrary::arbitrary(g);
            Timestamp { time: w, count: l }
        }
        fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            Box::new((self.time, self.count).shrink()
                    .map(|(w, l)| Timestamp { time: w, count: l }))
        }
    }

    #[test]
    fn fig_6_proc_0_a() {
        let src = ManualClock(Cell::new(0));
        let mut clock = Clock::new(&src);
        src.0.set(10);
        assert_eq!(clock.on_send(), Timestamp { time: 10, count: 0 })
    }

    #[test]
    fn fig_6_proc_1_a() {
        let src = ManualClock(Cell::new(1));
        let mut clock = Clock::new(&src);
        assert_eq!(clock.on_recv(&Timestamp { time: 10, count: 0 }).unwrap(), Timestamp { time: 10, count: 1 })
    }

    #[test]
    fn fig_6_proc_1_b() {
        let src = ManualClock(Cell::new(1));
        let mut clock = Clock::new(&src);
        let _ = clock.on_recv(&Timestamp { time: 10, count: 0 }).unwrap();
        src.0.set(2);
        assert_eq!(clock.on_send(), Timestamp { time: 10, count: 2 })
    }

    #[test]
    fn fig_6_proc_2_b() {
        let src = ManualClock(Cell::new(0));
        let mut clock = Clock::new(&src);
        clock.latest = Timestamp { time: 1, count: 0 };
        src.0.set(2);
        assert_eq!(clock.on_recv(&Timestamp { time: 10, count: 2 }).unwrap(), Timestamp { time: 10, count: 3 })
    }

    #[test]
    fn fig_6_proc_2_c() {
        let src = ManualClock(Cell::new(0));
        let mut clock = Clock::new(&src);
        src.0.set(2);
        let _ = clock.on_recv(&Timestamp { time: 10, count: 2 }).unwrap();
        src.0.set(3);
        assert_eq!(clock.on_send(), Timestamp { time: 10, count: 4 })
    }

    #[test]
    fn all_sources_same() {
        let src = ManualClock(Cell::new(0));
        let mut clock = Clock::new(&src);
        assert_eq!(clock.on_recv(&Timestamp { time: 0, count: 5 }).unwrap(), Timestamp { time: 0, count: 6 })
    }

    #[test]
    fn handles_time_going_backwards_on_send() {
        let src = ManualClock(Cell::new(10));
        let mut clock = Clock::new(&src);
        let _ = clock.on_send();
        src.0.set(9);
        assert_eq!(clock.on_send(), Timestamp { time: 10, count: 2 })
    }

    #[test]
    fn handles_time_going_backwards_on_recv() {
        let src = ManualClock(Cell::new(10));
        let mut clock = Clock::new(&src);
        let _ = clock.on_send();
        src.0.set(9);
        assert_eq!(clock.on_recv(&Timestamp { time: 0, count: 0 }).unwrap(), Timestamp { time: 10, count: 2 })
    }

    #[test]
    fn handles_time_going_forwards_on_send() {
        let src = ManualClock(Cell::new(10));
        let mut clock = Clock::new(&src);
        let t = clock.on_send();
        println!("at 10: {}", t);
        src.0.set(12);
        let t2 = clock.on_send();
        println!("=> 12: {}", t2);
        assert_eq!(t2, Timestamp { time: 12, count: 0 })
    }

    #[test]
    fn handles_time_going_forwards_on_recv() {
        let src = ManualClock(Cell::new(10));
        let mut clock = Clock::new(&src);
        let _ = clock.on_send();
        src.0.set(12);
        assert_eq!(clock.on_recv(&Timestamp { time: 0, count: 0 }).unwrap(), Timestamp { time: 12, count: 0 })
    }

    #[test]
    fn should_ignore_clocks_too_far_forward() {
        let src = ManualClock(Cell::new(0));
        let mut clock = Clock::new_with_max_diff(&src, 10);
        assert!(clock.on_recv(&Timestamp { time: 11, count: 0 }).is_err());
        assert_eq!(clock.on_recv(&Timestamp { time: 1, count: 0 }).unwrap(), Timestamp { time: 1, count: 1 })
    }

    #[test]
    fn should_account_for_time_passing_when_checking_max_error() {
        let src = ManualClock(Cell::new(0));
        let mut clock = Clock::new_with_max_diff(&src, 10);
        src.0.set(1);
        assert!(clock.on_recv(&Timestamp { time: 11, count: 0 }).is_ok());
    }


    #[test]
    fn should_round_trip_via_key() {
        fn prop(ts: Timestamp<WallT>) -> bool {
            let mut bs = Vec::new();
            ts.write_bytes(&mut bs).expect("write_bytes");
            let ts2 = Timestamp::read_bytes(Cursor::new(&bs)).expect("read_bytes");
            // println!("{:?}\t{:?}", ts == ts2, bs);
            ts == ts2
        }

        quickcheck::quickcheck(prop as fn(Timestamp<WallT>) -> bool)
    }

    #[test]
    fn byte_repr_should_order_as_timestamps() {
        fn prop(ta: Timestamp<WallT>, tb: Timestamp<WallT>) -> bool {
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
        }

        quickcheck::quickcheck(prop as fn(Timestamp<WallT>, Timestamp<WallT>) -> bool)
    }

    #[cfg(feature = "serde")]
    mod serde {
        use serde_json;
        use Clock;
        #[test]
        fn wall_timestamps_can_be_serialized() {
            let mut wall = Clock::wall();
            let ts = wall.on_send();
            let s = serde_json::to_string(&ts).expect("to-json");
            let ts2 = serde_json::from_str(&s).expect("from-json");
            assert_eq!(ts, ts2);
        }
    }
}
