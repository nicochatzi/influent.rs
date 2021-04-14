use std::borrow::Cow;
use std::collections::BTreeMap;

/// Point's field value.
#[derive(Debug)]
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

impl<'a> ToString for Value<'a> {
    fn to_string(&self) -> String {
        match *self {
            Value::String(s) => format!("\"{}\"", s.replace("\"", "\\\"")),
            Value::Integer(i) => format!("{}i", i),
            Value::Float(f) => f.to_string(),
            Value::Boolean(b) => if b { "t" } else { "f" }.to_string(),
        }
    }
}

/// Point model.
#[derive(Debug)]
pub struct Point<'a> {
    /// Key.
    pub key: &'a str,

    /// Timestamp.
    pub timestamp: Option<i64>,

    /// Map of fields.
    pub fields: BTreeMap<Cow<'a, str>, Value<'a>>,

    /// Map of tags.
    pub tags: BTreeMap<Cow<'a, str>, Cow<'a, str>>,
}

impl<'a> Point<'a> {
    /// Constructs a new `Point`.
    ///
    /// # Examples
    ///
    /// ```
    /// use influent::Point::Point;
    ///
    /// let Point = Point::new("key");
    /// ```
    pub fn new(key: &str) -> Point {
        Point {
            key,
            timestamp: None,
            fields: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }

    /// Adds field to the Point.
    ///
    /// # Examples
    ///
    /// ```
    /// use influent::Point::{Point, Value};
    ///
    /// let Point = Point::new("key")
    ///     .field("field", Value::String("hello"));
    /// ```
    pub fn field<T>(mut self, field: T, value: Value<'a>) -> Self
    where
        T: Into<Cow<'a, str>>,
    {
        self.fields.insert(field.into(), value);
        self
    }

    /// Adds tag to the Point.
    ///
    /// # Examples
    ///
    /// ```
    /// use influent::Point::{Point, Value};
    ///
    /// let Point = Point::new("key")
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

    /// Sets the timestamp of the Point. It should be unix timestamp in nanosecond
    ///
    /// # Examples
    ///
    /// ```
    /// use influent::Point::{Point, Value};
    ///
    /// let Point = Point::new("key")
    ///     .timestamp(1434055562000000000);
    /// ```
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Sets the timestamp of the Point as the current unix timestamp in nanosecond
    ///
    /// # Examples
    ///
    /// ```
    /// use influent::Point::{Point, Value};
    ///
    /// let Point = Point::new("key").now();
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

impl<'a> ToString for Point<'a> {
    fn to_string(&self) -> String {
        let mut line = vec![escape(self.key)];

        for (tag, value) in &self.tags {
            line.push(",".to_string());
            line.push(escape(tag));
            line.push("=".to_string());
            line.push(escape(value));
        }

        let mut was_spaced = false;

        for (field, value) in &self.fields {
            line.push(
                {
                    if !was_spaced {
                        was_spaced = true;
                        " "
                    } else {
                        ","
                    }
                }
                .to_string(),
            );
            line.push(escape(field));
            line.push("=".to_string());
            line.push(value.to_string());
        }

        if let Some(t) = self.timestamp {
            line.push(" ".to_string());
            line.push(t.to_string());
        }

        line.join("")
    }
}

fn escape(s: &str) -> String {
    s.replace(" ", "\\ ").replace(",", "\\,")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn Point_is_buildable() {
        const KEY: &str = "test-key";
        const TIMESTAMP: i64 = 20;

        let Point = Point::new(KEY).timestamp(TIMESTAMP);

        assert_eq!(KEY, Point.key);
        assert_eq!(TIMESTAMP, Point.timestamp.unwrap());
    }

    #[test]
    fn Point_is_in_time() {
        let earlier = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        let Point = Point::new("").now();

        assert!(earlier <= Point.timestamp.unwrap())
    }

    #[test]
    fn boolean_value_serializes() {
        assert_eq!("t", Value::Boolean(true).to_string());
        assert_eq!("f", Value::Boolean(true).to_string());
    }

    #[test]
    fn string_value_serializes() {
        assert_eq!("\"\\\"hello\\\"\"", Value::String("\"hello\"").to_string());
    }

    #[test]
    fn integer_value_serializes() {
        assert_eq!("1i", Value::Integer(1i64).to_string());
        assert_eq!("345i", Value::Integer(345i64).to_string());
        assert_eq!("2015i", Value::Integer(2015i64).to_string());
        assert_eq!("-10i", Value::Integer(-10i64).to_string());
    }

    #[test]
    fn float_value_serializes() {
        assert_eq!("1", Value::Float(1f64).to_string());
        assert_eq!("1", Value::Float(1.0f64).to_string());
        assert_eq!("-3.14", Value::Float(-3.14f64).to_string());
        assert_eq!("10", Value::Float(10f64).to_string());
    }

    #[test]
    fn test_escape() {
        assert_eq!("\\ ", escape(" "));
        assert_eq!("\\,", escape(","));
        assert_eq!("hello\\,\\ gobwas", escape("hello, gobwas"));
    }

    #[test]
    fn test_line_serializer() {
        let Point = Point::new("key")
            .field("s", Value::String("string"))
            .field("i", Value::Integer(10))
            .field("f", Value::Float(10f64))
            .field("b", Value::Boolean(false))
            .tag("tag", "value")
            .field("one, two", Value::String("three"))
            .tag("one ,two", "three, four")
            .timestamp(10);
        // r#"key,one ,two=three, four,tag=value b=f,f=10,i=10i,one, two="three",s="string" 10"#,
        assert_eq!("key,one\\ \\,two=three\\,\\ four,tag=value b=f,f=10,i=10i,one\\,\\ two=\"three\",s=\"string\" 10", Point.to_string());
    }

    #[test]
    fn test_line_serializer_long_timestamp() {
        let Point = Point::new("key")
            .field("s", Value::String("string"))
            .timestamp(1434055562000000000);

        assert_eq!("key s=\"string\" 1434055562000000000", Point.to_string());
    }
}
