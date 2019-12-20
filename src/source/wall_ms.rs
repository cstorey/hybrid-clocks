use std::convert::TryInto;
use std::fmt;
use std::ops::Sub;
use std::time::{Duration, SystemTime};

use super::{ClockSource, NANOS_PER_SEC};
use crate::{Result, Timestamp};

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
    /// Returns a `SystemTime` representing this timestamp.
    pub fn duration_since_epoch(self) -> Duration {
        // TODO: use Duration::from_nanos
        let nanos_per_tick = NANOS_PER_SEC / Self::TICKS_PER_SEC;
        let secs = self.0 / Self::TICKS_PER_SEC;
        let minor_ticks = self.0 % Self::TICKS_PER_SEC;
        let nsecs = minor_ticks * nanos_per_tick;
        assert!(nsecs < 1000_000_000, "Internal arithmetic error");
        Duration::new(secs, nsecs.try_into().expect("internal error"))
    }

    pub fn as_timespec(self) -> SystemTime {
        SystemTime::UNIX_EPOCH + self.duration_since_epoch()
    }

    /// Returns a `WallMST` representing the `SystemTime`.
    pub fn from_timespec(t: SystemTime) -> Result<Self> {
        // TODO: use Duration::as_nanos
        let since_epoch = t.duration_since(SystemTime::UNIX_EPOCH)?;
        Ok(Self::from_since_epoch(since_epoch))
    }

    pub fn from_since_epoch(since_epoch: Duration) -> Self {
        let nanos_per_tick = NANOS_PER_SEC / Self::TICKS_PER_SEC;
        let major_ticks = since_epoch.as_secs() * Self::TICKS_PER_SEC;
        let minor_ticks = u64::from(since_epoch.subsec_nanos()) / nanos_per_tick;
        WallMST(major_ticks + minor_ticks)
    }

    /// Returns time in nanoseconds since the unix epoch.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns time in nanoseconds since the unix epoch.
    pub fn of_u64(val: u64) -> Self {
        WallMST(val)
    }
}

impl Sub for WallMST {
    type Output = Duration;
    fn sub(self, rhs: Self) -> Self::Output {
        self.duration_since_epoch() - rhs.duration_since_epoch()
    }
}

impl ClockSource for WallMS {
    type Time = WallMST;
    type Delta = Duration;
    fn now(&mut self) -> Result<Self::Time> {
        WallMST::from_timespec(SystemTime::now())
    }
}

impl fmt::Display for WallMST {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{}", self.duration_since_epoch().as_secs_f64(),)
    }
}
#[cfg(test)]
mod tests {
    use super::WallMST;
    use crate::tests::timestamps;
    use crate::Timestamp;
    use suppositions::generators::*;

    use suppositions::*;

    fn wallclocks2() -> Box<dyn GeneratorObject<Item = WallMST>> {
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
            let wc2 = WallMST::from_timespec(tsp).expect("from time");
            assert_eq!(wc, wc2, "left:{}; tsp: {:?}; right:{}", wc, tsp, wc2);
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
