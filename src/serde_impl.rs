use serde::{ser, de};
use super::{Timestamp, WallT};

impl<T: ser::Serialize> ser::Serialize for Timestamp<T> {
    fn serialize<S: ser::Serializer>(&self, serializer: &mut S) -> Result<(), S::Error> {
        let mut tuple_state = try!(serializer.serialize_tuple_struct("Timestamp", 3usize));
        try!(serializer.serialize_tuple_struct_elt(&mut tuple_state, &self.epoch));
        try!(serializer.serialize_tuple_struct_elt(&mut tuple_state, &self.time));
        try!(serializer.serialize_tuple_struct_elt(&mut tuple_state, &self.count));
        return serializer.serialize_tuple_struct_end(tuple_state);
    }
}
impl<T: de::Deserialize> de::Deserialize for Timestamp<T> {
    fn deserialize<D>(deserializer: &mut D) -> ::std::result::Result<Timestamp<T>, D::Error>
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
            fn visit_seq<V>(&mut self,
                            mut visitor: V)
                            -> ::std::result::Result<Timestamp<T>, V::Error>
                where V: de::SeqVisitor
            {
                {
                    let field0 = match try!(visitor.visit()) {
                        Some(value) => value,
                        None => {
                            return Err(de::Error::end_of_stream());
                        }
                    };
                    let field1 = match try!(visitor.visit()) {
                        Some(value) => value,
                        None => {
                            return Err(de::Error::end_of_stream());
                        }
                    };
                    let field2 = match try!(visitor.visit()) {
                        Some(value) => value,
                        None => {
                            return Err(de::Error::end_of_stream());
                        }
                    };

                    try!(visitor.end());
                    Ok(Timestamp { epoch: field0, time: field1, count: field2 })
                }
            }
        }
        deserializer.deserialize_tuple_struct("Timestamp",
                                              3usize,
                                              Visitor::<D, T>(::std::marker::PhantomData,
                                                              ::std::marker::PhantomData))
    }
}

impl ser::Serialize for WallT {
    fn serialize<S: ser::Serializer>(&self, serializer: &mut S) -> Result<(), S::Error> {
        let mut tuple_state = try!(serializer.serialize_tuple_struct("WallT", 1usize));
        try!(serializer.serialize_tuple_struct_elt(&mut tuple_state, &self.0));
        return serializer.serialize_tuple_struct_end(tuple_state);
    }
}
impl de::Deserialize for WallT {
    fn deserialize<D>(deserializer: &mut D) -> ::std::result::Result<WallT, D::Error>
        where D: de::Deserializer
    {
        struct Visitor<D: de::Deserializer>(::std::marker::PhantomData<D>);
        impl<D: de::Deserializer> de::Visitor for Visitor<D> {
            type
            Value
            =
            WallT;
            #[inline]
            fn visit_seq<V>(&mut self, mut visitor: V) -> ::std::result::Result<WallT, V::Error>
                where V: de::SeqVisitor
            {
                {
                    let field0 = match try!(visitor.visit()) {
                        Some(value) => value,
                        None => {
                            return Err(de::Error::end_of_stream());
                        }
                    };
                    try!(visitor.end());
                    Ok(WallT(field0))
                }
            }
        }
        deserializer.deserialize_tuple_struct("WallT",
                                              1usize,
                                              Visitor::<D>(::std::marker::PhantomData))
    }
}
