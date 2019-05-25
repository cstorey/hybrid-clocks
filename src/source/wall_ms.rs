use std::convert::TryInto;
use std::fmt;
use std::ops::Sub;
use time::Duration;

use super::{ClockSource, NANOS_PER_SEC};
use crate::Timestamp;

// A clock source that returns wall-clock in 2^(-16)s
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WallMS;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serialization", derive(Serialize, Deserialize))]
pub struct WallMST(u64);

impl Timestamp<WallMST> {
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut res = [0; 16];
        res[0..4].copy_from_slice(&self.epoch.to_be_bytes());
        res[4..12].copy_from_slice(&self.time.0.to_be_bytes());
        res[12..16].copy_from_slice(&self.count.to_be_bytes());
        return res;
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let epoch = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let nanos = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
        let count = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        Timestamp {
            epoch: epoch,
            time: WallMST(nanos),
            count: count,
        }
    }
}
impl WallMST {
    const TICKS_PER_SEC: u64 = 1 << 16;
    /// Returns a `time::Timespec` representing this timestamp.
    pub fn as_timespec(self) -> time::Timespec {
        let nanos_per_tick = NANOS_PER_SEC / Self::TICKS_PER_SEC;
        let secs = self.0 / Self::TICKS_PER_SEC;
        let minor_ticks = self.0 % Self::TICKS_PER_SEC;
        let nsecs = minor_ticks * nanos_per_tick;
        time::Timespec {
            sec: secs as i64,
            nsec: nsecs as i32,
        }
    }

    /// Returns a `WallMST` representing the `time::Timespec`.
    fn from_timespec(t: time::Timespec) -> Self {
        let nanos_per_tick = NANOS_PER_SEC / Self::TICKS_PER_SEC;
        let major_ticks = t.sec as u64 * Self::TICKS_PER_SEC;
        let minor_ticks = t.nsec as u64 / nanos_per_tick;
        WallMST(major_ticks + minor_ticks)
    }

    /// Returns time in nanoseconds since the unix epoch.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl Sub for WallMST {
    type Output = Duration;
    fn sub(self, rhs: Self) -> Self::Output {
        self.as_timespec() - rhs.as_timespec()
    }
}

impl ClockSource for WallMS {
    type Time = WallMST;
    type Delta = Duration;
    fn now(&mut self) -> Self::Time {
        WallMST::from_timespec(time::get_time())
    }
}

impl fmt::Display for WallMST {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let tm = time::at_utc(self.as_timespec());
        write!(
            fmt,
            "{}",
            tm.strftime("%Y-%m-%dT%H:%M:%S.%fZ").expect("strftime")
        )
    }
}
#[cfg(test)]
mod tests {
    use super::WallMST;
    use crate::tests::timestamps;
    use crate::Timestamp;
    use suppositions::generators::*;

    use suppositions::*;

    fn wallclocks2() -> Box<GeneratorObject<Item = WallMST>> {
        u64s().map(WallMST).boxed()
    }

    #[test]
    fn should_round_trip_via_key() {
        property(timestamps(wallclocks2())).check(|ts| {
            let bs = ts.to_bytes();
            let ts2 = Timestamp::<WallMST>::from_bytes(bs);
            // println!("{:?}\t{:?}", ts == ts2, bs);
            ts == ts2
        });
    }

    #[test]
    fn should_round_trip_via_timespec() {
        property(wallclocks2()).check(|wc| {
            let tsp = wc.as_timespec();
            let wc2 = WallMST::from_timespec(tsp);
            assert_eq!(
                wc,
                wc2,
                "left:{}; tsp: {}; right:{}",
                wc,
                time::at_utc(tsp)
                    .strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    .expect("strftime"),
                wc2
            );
        });
    }

    #[test]
    fn timespec_should_order_as_timestamps() {
        property((wallclocks2(), wallclocks2())).check(|(ta, tb)| {
            use std::cmp::Ord;

            let ba = ta.as_timespec();
            let bb = tb.as_timespec();
            ta.cmp(&tb) == ba.cmp(&bb)
        })
    }

    #[test]
    fn byte_repr_should_order_as_timestamps() {
        property((timestamps(wallclocks2()), timestamps(wallclocks2()))).check(|(ta, tb)| {
            use std::cmp::Ord;

            let ba = ta.to_bytes();
            let bb = tb.to_bytes();
            ta.cmp(&tb) == ba.cmp(&bb)
        })
    }
}
