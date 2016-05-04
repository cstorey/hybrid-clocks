use serde::{ser, de};
use super::{Timestamp, WallT};
use time;

impl<T: ser::Serialize> ser::Serialize for Timestamp<T> {
    fn serialize<S: ser::Serializer>(&self, serializer: &mut S) -> Result<(), S::Error> {
        struct Visitor<'a, T: ser::Serialize + 'a> {
            state: usize,
            value: &'a Timestamp<T>,
            _structure_ty: ::std::marker::PhantomData<&'a Timestamp<T>>,
        }
        impl<'a, T: ser::Serialize + 'a> ser::SeqVisitor for Visitor<'a, T> {
            #[inline]
            fn visit<S>(&mut self,
                        serializer: &mut S)
                        -> ::std::result::Result<Option<()>, S::Error>
                where S: ser::Serializer
            {
                match self.state {
                    0usize => {
                        self.state += 1;
                        Ok(Some(try!(serializer.serialize_tuple_struct_elt(&self.value.epoch))))
                    }
                    1usize => {
                        self.state += 1;
                        Ok(Some(try!(serializer.serialize_tuple_struct_elt(&self.value.time))))
                    }
                    2usize => {
                        self.state += 1;
                        Ok(Some(try!(serializer.serialize_tuple_struct_elt(&self.value.count))))
                    }
                    _ => Ok(None),
                }
            }
            #[inline]
            fn len(&self) -> Option<usize> {
                Some(3usize)
            }
        }

        serializer.serialize_tuple_struct("Timestamp",
                                          Visitor {
                                              value: self,
                                              state: 0,
                                              _structure_ty:
                                                  ::std::marker::PhantomData::<&Timestamp<T>>,
                                          })
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
        struct Visitor<'a> {
            state: usize,
            value: &'a WallT,
            _structure_ty: ::std::marker::PhantomData<&'a WallT>,
        }
        impl<'a> ser::SeqVisitor for Visitor<'a> {
            #[inline]
            fn visit<S>(&mut self,
                        serializer: &mut S)
                        -> ::std::result::Result<Option<()>, S::Error>
                where S: ser::Serializer
            {
                match self.state {
                    0usize => {
                        self.state += 1;
                        Ok(Some(try!(serializer.serialize_tuple_struct_elt(&self.value.0))))
                    }
                    _ => Ok(None),
                }
            }
            #[inline]
            fn len(&self) -> Option<usize> {
                Some(1usize)
            }
        }

        serializer.serialize_tuple_struct("WallT",
                                          Visitor {
                                              value: self,
                                              state: 0,
                                              _structure_ty: ::std::marker::PhantomData::<&WallT>,
                                          })
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
