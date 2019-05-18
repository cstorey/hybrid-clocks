use std::convert::TryInto;
use std::fmt;
use std::io;
use std::ops::Sub;
use time::Duration;

use super::ClockSource;
use super::NANOS_PER_SEC;
use crate::Timestamp;

/// A clock source that returns wall-clock in nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Wall;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serialization", derive(Serialize, Deserialize))]
pub struct WallT(u64);

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

    fn of_nanos(nanos: u64) -> Self {
        WallT(nanos)
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
            time: WallT::of_nanos(nanos),
            count: count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WallT;
    use crate::tests::timestamps;
    use crate::Timestamp;
    use std::io::Cursor;
    use suppositions::generators::*;

    use suppositions::*;

    fn wallclocks() -> Box<GeneratorObject<Item = WallT>> {
        u64s().map(WallT::of_nanos).boxed()
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

}
