use std::convert::TryInto;
use std::fmt;
use std::ops::Sub;
use std::time::{Duration, SystemTime};

use super::{ClockSource, NANOS_PER_SEC};
use crate::{Error, Result, Timestamp};

// A clock source that returns wall-clock in 2^(-16)s
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WallMS;
/// Representation of our timestamp.
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
    pub fn duration_since_epoch(self) -> Result<Duration> {
        // TODO: use Duration::from_nanos
        let nanos_per_tick = NANOS_PER_SEC / Self::TICKS_PER_SEC;
        let secs = self.0 / Self::TICKS_PER_SEC;
        let minor_ticks = self.0 % Self::TICKS_PER_SEC;
        let nsecs = minor_ticks * nanos_per_tick;
        assert!(nsecs < 1000_000_000, "Internal arithmetic error");
        Duration::new(secs, nsecs.try_into().expect("internal error"));

        let nanos = u128::from(self.0)
            .checked_mul(u128::from(NANOS_PER_SEC))
            .ok_or_else(|| Error::SupportedTime(self.0.into()))?
            / u128::from(Self::TICKS_PER_SEC);

        Ok(Duration::from_nanos(
            nanos
                .try_into()
                .map_err(|_| Error::SupportedTime(nanos.into()))?,
        ))
    }

    pub fn as_systemtime(self) -> Result<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH + self.duration_since_epoch()?)
    }

    /// Returns a `WallMST` representing the `SystemTime`.
    pub fn from_timespec(t: SystemTime) -> Result<Self> {
        // TODO: use Duration::as_nanos
        let since_epoch = t.duration_since(SystemTime::UNIX_EPOCH)?;
        Self::from_since_epoch(since_epoch)
    }

    pub fn from_since_epoch(since_epoch: Duration) -> Result<Self> {
        let ticks: u128 = u128::from(Self::TICKS_PER_SEC)
            .checked_mul(since_epoch.as_nanos())
            .ok_or_else(|| Error::SupportedTime(since_epoch.as_nanos()))?
            / u128::from(NANOS_PER_SEC);
        Ok(WallMST(ticks.try_into()?))
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
        let nanos = (self.0 - rhs.0)
            .checked_mul(NANOS_PER_SEC / Self::TICKS_PER_SEC)
            .expect("inside time range");
        Duration::from_nanos(nanos)
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
    #[cfg(not(feature = "pretty-print"))]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.duration_since_epoch() {
            Ok(epoch) => write!(fmt, "{}", epoch.as_secs_f64()),
            Err(e) => write!(fmt, "{}", e),
        }
    }

    #[cfg(feature = "pretty-print")]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_systemtime() {
            Ok(ts) => {
                let st = time::PrimitiveDateTime::from(ts);
                write!(
                    fmt,
                    "{}.{:09}Z",
                    st.format("%Y-%m-%dT%H:%M:%S"),
                    st.nanosecond(),
                )
            }
            Err(e) => write!(fmt, "{}", e),
        }
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
        u64s()
            .map(|val| {
                let limit = u64::max_value() / 200000;
                let scaled = (u128::from(val) * u128::from(limit)) >> 64;
                eprintln!(
                    "val:{} * limit:{} -> {}; scaled:{}",
                    u128::from(val),
                    u128::from(limit),
                    u128::from(val) * u128::from(limit),
                    scaled
                );
                eprintln!("{}", WallMST(scaled as u64));
                WallMST(scaled as u64)
            })
            .boxed()
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
        // We expect millisecond precision, so ensure we're within ± 0.5ms
        let allowable_error = WallMST::TICKS_PER_SEC / 1000 / 2;

        property(wallclocks2()).check(|wc| {
            let tsp = wc.as_systemtime().expect("wall time");
            let wc2 = WallMST::from_timespec(tsp).expect("from time");
            let diff = wc.0 - wc2.0;
            assert!(
                diff <= allowable_error,
                "left:{}; tsp: {:?}; right:{}; diff:{}",
                wc,
                tsp,
                wc2,
                diff
            );
        });
    }

    #[test]
    fn timespec_should_order_as_timestamps() {
        property((wallclocks2(), wallclocks2())).check(|(ta, tb)| {
            use std::cmp::Ord;

            let ba = ta.as_systemtime().expect("wall time");
            let bb = tb.as_systemtime().expect("wall time");
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
