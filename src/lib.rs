extern crate time;
extern crate byteorder;
#[cfg(test)]
extern crate quickcheck;

#[cfg(feature = "serde")]
extern crate serde;
#[cfg(all(feature = "serde", test))]
extern crate serde_json;

use std::cmp;
use std::fmt;
use std::io;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};


// Implementation of Hybrid Logical Clocks, based on the paper "Logical Physical Clocks
// and Consistent Snapshots in Globally Distributed Databases".
//

pub trait ClockSource {
    type Time : Ord + Copy;
    fn now(&mut self) -> Self::Time;
}

#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord)]
pub struct Timestamp<T>(T, u32);

#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord)]
pub struct Wall;
#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord)]
// Nanoseconds since unix epoch
pub struct WallT(u64);

pub struct Clock<S: ClockSource> {
    src: S,
    latest: Timestamp<S::Time>,
}

impl Clock<Wall> {
    pub fn wall() -> Clock<Wall> {
        Clock::new(Wall)
    }
}

impl<S: ClockSource> Clock<S> {
    pub fn new(mut src: S) -> Self {
        let init = src.now();
        Clock {
            src: src,
            latest: Timestamp(init, 0),
        }
    }

    pub fn on_send(&mut self) -> Timestamp<S::Time> {
        let pt = self.src.now();
        let lp = self.latest.clone();
        self.latest.0 = cmp::max(lp.0, pt);
        self.latest.1 = if lp.0 == self.latest.0 {
            lp.1 + 1
        } else {
            0
        };

        self.latest
    }

    pub fn on_recv(&mut self, msg: &Timestamp<S::Time>) -> Timestamp<S::Time> {
        let pt = self.src.now();
        let lp = self.latest.clone();

        self.latest.0 = cmp::max(cmp::max(lp.0, msg.0), pt);
        self.latest.1 = match (self.latest.0 == lp.0, self.latest.0 == msg.0) {
            (true, true) => cmp::max(self.latest.1, msg.1) + 1,
            (true, false) => self.latest.1 + 1,
            (false, true) => msg.1 + 1,
            (false, false) => 0,
        };

        self.latest.clone()
    }
}

impl<T> Timestamp<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl Timestamp<WallT> {
    pub fn write_bytes<W: io::Write>(&self, mut wr: W) -> Result<(), io::Error> {
        let wall = &self.0;
        try!(wr.write_u64::<BigEndian>(wall.0));
        try!(wr.write_u32::<BigEndian>(self.1));
        Ok(())
    }

    pub fn read_bytes<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        // use ClockSource;
        let nanos = try!(r.read_u64::<BigEndian>());
        let l = try!(r.read_u32::<BigEndian>());
        let wall = WallT(nanos);
        Ok(Timestamp(wall, l))
    }
}

const NANOS_PER_SEC : u64 = 1000_000_000;

impl WallT {
    pub fn as_ts(self) -> time::Timespec {
        let secs = self.0 / NANOS_PER_SEC;
        let nsecs = self.0 % NANOS_PER_SEC;
        time::Timespec { sec: secs as i64, nsec: nsecs as i32 }
    }

    fn from_ts(t: time::Timespec) -> Self {
        WallT(t.sec as u64 * NANOS_PER_SEC + t.nsec as u64)
    }
}


impl ClockSource for Wall {
    type Time = WallT;
    fn now(&mut self) -> Self::Time {
        WallT::from_ts(time::get_time())
    }
}

impl fmt::Display for WallT {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let tm = time::at_utc(self.as_ts());
        write!(fmt, "{}", tm.strftime("%Y-%m-%dT%H:%M:%S.%fZ").expect("strftime"))
    }
}

impl<T: fmt::Display> fmt::Display for Timestamp<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}+{}", self.0, self.1)
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
            Timestamp(w, l)
        }
        fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            Box::new((self.0, self.1).shrink()
                    .map(|(w, l)| Timestamp(w, l)))
        }
    }

    #[test]
    fn fig_6_proc_0_a() {
        let src = ManualClock(Cell::new(0));
        let mut clock = Clock::new(&src);
        src.0.set(10);
        assert_eq!(clock.on_send(), Timestamp(10, 0))
    }

    #[test]
    fn fig_6_proc_1_a() {
        let src = ManualClock(Cell::new(1));
        let mut clock = Clock::new(&src);
        assert_eq!(clock.on_recv(&Timestamp(10, 0)), Timestamp(10, 1))
    }

    #[test]
    fn fig_6_proc_1_b() {
        let src = ManualClock(Cell::new(1));
        let mut clock = Clock::new(&src);
        let _ = clock.on_recv(&Timestamp(10, 0));
        src.0.set(2);
        assert_eq!(clock.on_send(), Timestamp(10, 2))
    }

    #[test]
    fn fig_6_proc_2_b() {
        let src = ManualClock(Cell::new(0));
        let mut clock = Clock::new(&src);
        clock.latest = Timestamp(1, 0);
        src.0.set(2);
        assert_eq!(clock.on_recv(&Timestamp(10, 2)), Timestamp(10, 3))
    }

    #[test]
    fn fig_6_proc_2_c() {
        let src = ManualClock(Cell::new(0));
        let mut clock = Clock::new(&src);
        src.0.set(2);
        let _ = clock.on_recv(&Timestamp(10, 2));
        src.0.set(3);
        assert_eq!(clock.on_send(), Timestamp(10, 4))
    }

    #[test]
    fn all_sources_same() {
        let src = ManualClock(Cell::new(0));
        let mut clock = Clock::new(&src);
        assert_eq!(clock.on_recv(&Timestamp(0, 5)), Timestamp(0, 6))
    }

    #[test]
    fn handles_time_going_backwards_on_send() {
        let src = ManualClock(Cell::new(10));
        let mut clock = Clock::new(&src);
        let _ = clock.on_send();
        src.0.set(9);
        assert_eq!(clock.on_send(), Timestamp(10, 2))
    }

    #[test]
    fn handles_time_going_backwards_on_recv() {
        let src = ManualClock(Cell::new(10));
        let mut clock = Clock::new(&src);
        let _ = clock.on_send();
        src.0.set(9);
        assert_eq!(clock.on_recv(&Timestamp(0, 0)), Timestamp(10, 2))
    }

    #[test]
    fn handles_time_going_forwards_on_send() {
        let src = ManualClock(Cell::new(10));
        let mut clock = Clock::new(&src);
        let _ = clock.on_send();
        src.0.set(12);
        assert_eq!(clock.on_send(), Timestamp(12, 0))
    }

    #[test]
    fn handles_time_going_forwards_on_recv() {
        let src = ManualClock(Cell::new(10));
        let mut clock = Clock::new(&src);
        let _ = clock.on_send();
        src.0.set(12);
        assert_eq!(clock.on_recv(&Timestamp(0, 0)), Timestamp(12, 0))
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
