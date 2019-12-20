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
pub struct WallNS;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serialization", derive(Serialize, Deserialize))]
pub struct WallNST(u64);

impl WallNST {
    /// Returns a `time::Timespec` representing this timestamp.
    pub fn as_timespec(self) -> time::Timespec {
        let secs = self.0 / NANOS_PER_SEC;
        let nsecs = self.0 % NANOS_PER_SEC;
        time::Timespec {
            sec: secs as i64,
            nsec: nsecs as i32,
        }
    }

    /// Returns a `WallNST` representing the `time::Timespec`.
    pub fn from_timespec(t: time::Timespec) -> Self {
        WallNST(t.sec as u64 * NANOS_PER_SEC + t.nsec as u64)
    }

    /// Returns time in nanoseconds since the unix epoch.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns time in nanoseconds since the unix epoch.
    pub fn of_u64(val: u64) -> Self {
        WallNST(val)
    }

    fn of_nanos(nanos: u64) -> Self {
        WallNST(nanos)
    }
}

impl Sub for WallNST {
    type Output = Duration;
    fn sub(self, rhs: Self) -> Self::Output {
        let nanos = self.0 - rhs.0;
        Duration::nanoseconds(nanos as i64)
    }
}

impl ClockSource for WallNS {
    type Time = WallNST;
    type Delta = Duration;
    fn now(&mut self) -> Self::Time {
        WallNST::from_timespec(time::get_time())
    }
}

impl fmt::Display for WallNST {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tm = time::at_utc(self.as_timespec());
        write!(
            fmt,
            "{}",
            tm.strftime("%Y-%m-%dT%H:%M:%S.%fZ").expect("strftime")
        )
    }
}

impl Timestamp<WallNST> {
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
            time: WallNST::of_nanos(nanos),
            count: count,
        }
    }
}
/// Serialization for the previous version.
#[cfg(all(feature = "serialization", feature = "deserialize-v1"))]
pub mod v1 {
    use std::fmt;

    use serde::ser::SerializeTupleStruct;
    use serde::{de, ser};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct WallNST(u64);

    impl From<super::WallNST> for WallNST {
        fn from(v2: super::WallNST) -> Self {
            return WallNST(v2.0);
        }
    }

    impl From<WallNST> for super::WallNST {
        fn from(v1: WallNST) -> super::WallNST {
            return super::WallNST(v1.0);
        }
    }

    impl ser::Serialize for WallNST {
        fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut tuple_state = serializer.serialize_tuple_struct("WallNST", 1usize)?;
            tuple_state.serialize_field(&self.0)?;
            return tuple_state.end();
        }
    }

    impl<'de> de::Deserialize<'de> for WallNST {
        fn deserialize<D>(deserializer: D) -> ::std::result::Result<WallNST, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            struct WallNSTVisitor;
            impl<'de> de::Visitor<'de> for WallNSTVisitor {
                type Value = WallNST;

                #[inline]
                fn visit_seq<V>(self, mut visitor: V) -> ::std::result::Result<WallNST, V::Error>
                where
                    V: de::SeqAccess<'de>,
                {
                    {
                        let field0 = match visitor.next_element()? {
                            Some(value) => value,
                            None => {
                                return Err(de::Error::invalid_length(
                                    0,
                                    &"Needed 1 values for WallNS clock",
                                ));
                            }
                        };
                        Ok(WallNST(field0))
                    }
                }

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("v1 WallNS clock value")
                }
            }

            deserializer.deserialize_tuple_struct("WallNST", 1usize, WallNSTVisitor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::timestamps;
    use crate::Timestamp;
    use std::io::Cursor;
    use suppositions::generators::*;

    use suppositions::*;

    fn wallclocks_ns() -> Box<dyn GeneratorObject<Item = WallNST>> {
        u64s().map(WallNST::of_nanos).boxed()
    }

    #[test]
    fn should_round_trip_via_key() {
        property(timestamps(wallclocks_ns())).check(|ts| {
            let mut bs = Vec::new();
            ts.write_bytes(&mut bs).expect("write_bytes");
            let ts2 = Timestamp::read_bytes(Cursor::new(&bs)).expect("read_bytes");
            // println!("{:?}\t{:?}", ts == ts2, bs);
            ts == ts2
        });
    }

    #[test]
    fn byte_repr_should_order_as_timestamps() {
        property((timestamps(wallclocks_ns()), timestamps(wallclocks_ns()))).check(|(ta, tb)| {
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

    #[cfg(feature = "serialization")]
    mod serde {
        use super::*;
        use serde_json;
        #[test]
        fn should_round_trip_via_serde() {
            property(timestamps(wallclocks_ns())).check(|ts| {
                let s = serde_json::to_string(&ts).expect("to-json");
                let ts2 = serde_json::from_str(&s).expect("from-json");
                ts == ts2
            });
        }

        #[test]
        fn should_round_trip_via_v1_serde() {
            property(timestamps(wallclocks_ns())).check(|ts| {
                let tsv1 = ts.time_into::<v1::WallNST>();
                let s = serde_json::to_string(&tsv1).expect("to-json");
                let ts2v1 = serde_json::from_str::<Timestamp<v1::WallNST>>(&s).expect("from-json");
                let ts2 = ts2v1.time_into::<WallNST>();
                ts == ts2
            });
        }

        #[cfg(feature = "deserialize-v1")]
        #[test]
        fn should_deserialize_v1() {
            let s = "[0,[1558805131923316000],0]";
            let ts = serde_json::from_str::<Timestamp<v1::WallNST>>(&s)
                .expect("from-json")
                .time_into();

            assert_eq!(
                ts,
                Timestamp {
                    epoch: 0,
                    time: WallNST(1558805131923316000),
                    count: 0,
                }
            )
        }
    }
}
