use std::borrow::Cow;
use std::collections::BTreeMap;

#[derive(Debug)]
/// Measurement's field value.
pub enum Value<'a> {
    /// String.
    String(&'a str),
    /// Floating point number.
    Float(f64),
    /// Integer number.
    Integer(i64),
    /// Boolean value.
    Boolean(bool),
}

/// Measurement model.
#[derive(Debug)]
pub struct Measurement<'a> {
    /// Key.
    pub key: &'a str,

    /// Timestamp.
    pub timestamp: Option<i64>,

    /// Map of fields.
    pub fields: BTreeMap<Cow<'a, str>, Value<'a>>,

    /// Map of tags.
    pub tags: BTreeMap<Cow<'a, str>, Cow<'a, str>>,
}

impl<'a> Measurement<'a> {
    /// Constructs a new `Measurement`.
    ///
    /// # Examples
    ///
    /// ```
    /// use influent::measurement::Measurement;
    ///
    /// let measurement = Measurement::new("key");
    /// ```
    pub fn new(key: &str) -> Measurement {
        Measurement {
            key,
            timestamp: None,
            fields: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }

    /// Adds field to the measurement.
    ///
    /// # Examples
    ///
    /// ```
    /// use influent::measurement::{Measurement, Value};
    ///
    /// let measurement = Measurement::new("key")
    ///     .field("field", Value::String("hello"));
    /// ```
    pub fn field<T>(mut self, field: T, value: Value<'a>) -> Self
    where
        T: Into<Cow<'a, str>>,
    {
        self.fields.insert(field.into(), value);
        self
    }

    /// Adds tag to the measurement.
    ///
    /// # Examples
    ///
    /// ```
    /// use influent::measurement::{Measurement, Value};
    ///
    /// let measurement = Measurement::new("key")
    ///     .tag("tag", "value");
    /// ```
    pub fn tag<I, K>(mut self, tag: I, value: K) -> Self
    where
        I: Into<Cow<'a, str>>,
        K: Into<Cow<'a, str>>,
    {
        self.tags.insert(tag.into(), value.into());
        self
    }

    /// Sets the timestamp of the measurement. It should be unix timestamp in nanosecond
    ///
    /// # Examples
    ///
    /// ```
    /// use influent::measurement::{Measurement, Value};
    ///
    /// let measurement = Measurement::new("key")
    ///     .timestamp(1434055562000000000);
    /// ```
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Sets the timestamp of the measurement as the current unix timestamp in nanosecond
    ///
    /// # Examples
    ///
    /// ```
    /// use influent::measurement::{Measurement, Value};
    ///
    /// let measurement = Measurement::new("key").now();
    /// ```
    pub fn now(self) -> Self {
        self.timestamp(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos() as i64,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn measurement_is_buildable() {
        const KEY: &str = "test-key";
        const TIMESTAMP: i64 = 20;

        let measurement = Measurement::new(KEY).timestamp(TIMESTAMP);

        assert_eq!(KEY, measurement.key);
        assert_eq!(TIMESTAMP, measurement.timestamp.unwrap());
    }

    #[test]
    fn measurement_is_time() {
        let earlier = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        let measurement = Measurement::new("").now();

        assert!(earlier <= measurement.timestamp.unwrap())
    }
}
