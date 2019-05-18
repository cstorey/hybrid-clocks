use super::{Wall2T, WallT};
use serde::ser::SerializeTupleStruct;
use serde::{de, ser};
use std::fmt;

#[derive(Serialize, Deserialize)]
struct Timestamp<T>(u32, T, u32);

impl<T: ser::Serialize + Copy> ser::Serialize for crate::Timestamp<T> {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self::Timestamp(self.epoch, self.time, self.count).serialize(serializer)
    }
}

impl<'de, T: de::Deserialize<'de>> de::Deserialize<'de> for crate::Timestamp<T> {
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<crate::Timestamp<T>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let self::Timestamp(epoch, time, count) = de::Deserialize::deserialize(deserializer)?;
        Ok(crate::Timestamp { epoch, time, count })
    }
}

impl ser::Serialize for WallT {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple_state = try!(serializer.serialize_tuple_struct("WallT", 1usize));
        try!(tuple_state.serialize_field(&self.0));
        return tuple_state.end();
    }
}

impl<'de> de::Deserialize<'de> for WallT {
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<WallT, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct WallTVisitor;
        impl<'de> de::Visitor<'de> for WallTVisitor {
            type Value = WallT;

            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> ::std::result::Result<WallT, V::Error>
            where
                V: de::SeqAccess<'de>,
            {
                {
                    let field0 = match try!(visitor.next_element()) {
                        Some(value) => value,
                        None => {
                            return Err(de::Error::invalid_length(
                                0,
                                &"Needed 1 values for wall clock",
                            ));
                        }
                    };
                    Ok(WallT(field0))
                }
            }

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a wall clock value")
            }
        }

        deserializer.deserialize_tuple_struct("WallT", 1usize, WallTVisitor)
    }
}
impl ser::Serialize for Wall2T {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple_state = try!(serializer.serialize_tuple_struct("Wall2T", 1usize));
        try!(tuple_state.serialize_field(&self.0));
        return tuple_state.end();
    }
}

impl<'de> de::Deserialize<'de> for Wall2T {
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<Wall2T, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct Wall2TVisitor;
        impl<'de> de::Visitor<'de> for Wall2TVisitor {
            type Value = Wall2T;

            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> ::std::result::Result<Wall2T, V::Error>
            where
                V: de::SeqAccess<'de>,
            {
                {
                    let field0 = match try!(visitor.next_element()) {
                        Some(value) => value,
                        None => {
                            return Err(de::Error::invalid_length(
                                0,
                                &"Needed 1 values for wall clock",
                            ));
                        }
                    };
                    Ok(Wall2T(field0))
                }
            }

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a wall clock value")
            }
        }

        deserializer.deserialize_tuple_struct("Wall2T", 1usize, Wall2TVisitor)
    }
}
