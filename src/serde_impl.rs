use serde::{de, ser};

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
