use serde::{ser, de};
use serde::ser::SerializeTupleStruct;
use super::{Timestamp, WallT};
use std::fmt;

impl<T: ser::Serialize> ser::Serialize for Timestamp<T> {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple_state = try!(serializer.serialize_tuple_struct("Timestamp", 3usize));
        try!(tuple_state.serialize_field(&self.epoch));
        try!(tuple_state.serialize_field(&self.time));
        try!(tuple_state.serialize_field(&self.count));
        return tuple_state.end();
    }
}
impl<T: de::Deserialize> de::Deserialize for Timestamp<T> {
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<Timestamp<T>, D::Error>
        where D: de::Deserializer
    {
        struct Visitor<D: de::Deserializer, T: de::Deserialize>(::std::marker::PhantomData<D>,
                                                                ::std::marker::PhantomData<T>);
        impl<D: de::Deserializer, T: de::Deserialize> de::Visitor for Visitor<D, T> {
            type
            Value
            =
            Timestamp<T>;
            #[inline]
            fn visit_seq<V>(self,
                            mut visitor: V)
                            -> ::std::result::Result<Timestamp<T>, V::Error>
                where V: de::SeqVisitor
            {
                {
                    let field0 = match try!(visitor.visit()) {
                        Some(value) => value,
                        None => {
                            return Err(de::Error::invalid_length(0, &"Needed 3 values for Timestamp"));
                        }
                    };
                    let field1 = match try!(visitor.visit()) {
                        Some(value) => value,
                        None => {
                            return Err(de::Error::invalid_length(1, &"Needed 3 values for Timestamp"));
                        }
                    };
                    let field2 = match try!(visitor.visit()) {
                        Some(value) => value,
                        None => {
                            return Err(de::Error::invalid_length(2, &"Needed 3 values for Timestamp"));
                        }
                    };

                    Ok(Timestamp { epoch: field0, time: field1, count: field2 })
                }
            }

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a timestamp")
            }
        }
        deserializer.deserialize_tuple_struct("Timestamp",
                                              3usize,
                                              Visitor::<D, T>(::std::marker::PhantomData,
                                                              ::std::marker::PhantomData))
    }
}

impl ser::Serialize for WallT {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple_state = try!(serializer.serialize_tuple_struct("WallT", 1usize));
        try!(tuple_state.serialize_field(&self.0));
        return tuple_state.end();
    }
}
impl de::Deserialize for WallT {
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<WallT, D::Error>
        where D: de::Deserializer
    {
        struct Visitor<D: de::Deserializer>(::std::marker::PhantomData<D>);
        impl<D: de::Deserializer> de::Visitor for Visitor<D> {
            type
            Value
            =
            WallT;
            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> ::std::result::Result<WallT, V::Error>
                where V: de::SeqVisitor
            {
                {
                    let field0 = match try!(visitor.visit()) {
                        Some(value) => value,
                        None => {
                            return Err(de::Error::invalid_length(0, &"Needed 1 values for wall clock"));
                        }
                    };
                    Ok(WallT(field0))
                }
            }

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a wall clock value")
            }
        }
        deserializer.deserialize_tuple_struct("WallT",
                                              1usize,
                                              Visitor::<D>(::std::marker::PhantomData))
    }
}
