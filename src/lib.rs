extern crate time;
#[cfg(test)]
extern crate quickcheck;

use std::cmp;
use std::fmt;

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
pub struct WallT(time::Timespec);

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

impl WallT {
    pub fn as_tm(self) -> time::Timespec {
        self.0
    }
}


impl ClockSource for Wall {
    type Time = WallT;
    fn now(&mut self) -> Self::Time {
        WallT(time::get_time())
    }
}

impl fmt::Display for WallT {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}.{:9}", self.0.sec, self.0.nsec)
    }
}

impl<T: fmt::Display> fmt::Display for Timestamp<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}+{}", self.0, self.1)
    }
}

#[cfg(test)]
mod tests {
    use super::{Clock, ClockSource, Timestamp};
    use std::cell::Cell;
    use std::cmp::{Ord, Ordering};
    use quickcheck;

    struct ManualClock(Cell<u64>);

    impl<'a> ClockSource for &'a ManualClock {
        type Time = u64;
        fn now(&mut self) -> Self::Time {
            self.0.get()
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
}
