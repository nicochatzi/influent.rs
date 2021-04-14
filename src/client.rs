use crate::hurl::{Auth, Hurl, Method, Request};
use crate::point::Point;
use async_trait::async_trait;
use std::collections::HashMap;
use std::io;

const MAX_BATCH: u16 = 5000;

fn db_var(key: &str) -> Result<String, std::env::VarError> {
    dotenv::dotenv().ok();
    std::env::var(format!("INFLUXDB_{}", key))
}

lazy_static::lazy_static! {
    static ref DB_BUCKET: String =
        db_var("BUCKET").expect("Could not find INFLUXDB_BUCKET in environment variables");
    static ref DB_USERNAME: String =
        db_var("USERNAME").expect("Could not find INFLUXDB_USERNAME in environment variables");
    static ref DB_PASSWORD: String =
        db_var("PASSWORD").expect("Could not find INFLUXDB_PASSWORD in environment variables");
    static ref DB_ADDRESS: String =
        db_var("ADDRESS").expect("Could not find INFLUXDB_ADDRESS in environment variables");
}

pub type ClientResult<T> = Result<T, ClientError>;

#[async_trait]
pub trait Client {
    async fn write_many(
        &self,
        line: &[Point<'_>],
        precision: Option<Precision>,
    ) -> ClientResult<()>;

    async fn write_one(&self, line: Point<'_>, precision: Option<Precision>) -> ClientResult<()>;

    async fn query(&self, query: String, precision: Option<Precision>) -> ClientResult<String>;
}

pub struct Credentials<'a> {
    pub username: &'a str,
    pub password: &'a str,
    pub database: &'a str,
}

pub enum Precision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
}

impl ToString for Precision {
    fn to_string(&self) -> String {
        match *self {
            Precision::Nanoseconds => "n",
            Precision::Microseconds => "u",
            Precision::Milliseconds => "ms",
            Precision::Seconds => "s",
            Precision::Minutes => "m",
            Precision::Hours => "h",
        }
        .to_string()
    }
}

#[derive(Debug)]
pub enum ClientError {
    CouldNotComplete(String),
    Communication(String),
    Syntax(String),
    Unexpected(String),
    Unknown,
}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        ClientError::Communication(format!("{}", e))
    }
}

pub enum WriteStatus {
    Success,
    CouldNotComplete,
}

// fixme
pub struct Options {
    pub max_batch: Option<u16>,
    pub precision: Option<Precision>,

    pub epoch: Option<Precision>,
    pub chunk_size: Option<u16>,
}

pub struct InfluxClient<'a> {
    credentials: Credentials<'a>,
    hurl: Box<dyn Hurl + Send + Sync>,
    host: &'a str,
    pub max_batch: u16,
}

impl<'a> Default for InfluxClient<'a> {
    ///
    /// Short-hand to construct an InfluxDB client from data
    /// stored in the env. This requires these vars:
    ///    INFLUXDB_BUCKET
    ///    INFLUXDB_USERNAME
    ///    INFLUXDB_PASSWORD
    ///    INFLUXDB_ADDRESS
    ///
    fn default() -> Self {
        Self {
            credentials: Credentials {
                username: &DB_USERNAME,
                password: &DB_PASSWORD,
                database: &DB_BUCKET,
            },
            host: &DB_ADDRESS,
            hurl: Box::new(crate::ReqwestHurl::default()),
            max_batch: MAX_BATCH,
        }
    }
}

impl<'a> InfluxClient<'a> {
    pub fn new(credentials: Credentials<'a>, host: &'a str) -> Self {
        Self {
            credentials,
            host,
            ..Default::default()
        }
    }

    pub fn set_hurl(&mut self, hurl: Box<dyn Hurl + Send + Sync>) {
        self.hurl = hurl;
    }
}

#[async_trait]
impl<'a> Client for InfluxClient<'a> {
    async fn query(&self, q: String, epoch: Option<Precision>) -> Result<String, ClientError> {
        let mut query = HashMap::new();
        query.insert("db", self.credentials.database.to_string());
        query.insert("q", q);

        if let Some(ref epoch) = epoch {
            query.insert("epoch", epoch.to_string());
        }

        let auth = if self.credentials.username == "" && self.credentials.password == "" {
            None
        } else {
            Some(Auth {
                username: self.credentials.username,
                password: self.credentials.password,
            })
        };

        let request = Request {
            url: &*{ self.host.to_owned() + "/query" },
            method: Method::GET,
            auth,
            query: Some(query),
            body: None,
        };

        let resp = self
            .hurl
            .request(request)
            .await
            .map_err(ClientError::Communication)?;
        match resp.status {
            200 => Ok(resp.to_string()),
            400 => Err(ClientError::Syntax(resp.to_string())),
            _ => Err(ClientError::Unexpected(format!(
                "Unexpected response. Status: {}; Body: \"{}\"",
                resp.status,
                resp.to_string()
            ))),
        }
    }

    async fn write_one(
        &self,
        measurement: Point<'_>,
        precision: Option<Precision>,
    ) -> Result<(), ClientError> {
        self.write_many(&[measurement], precision).await
    }

    async fn write_many(
        &self,
        measurements: &[Point<'_>],
        precision: Option<Precision>,
    ) -> Result<(), ClientError> {
        for chunk in measurements.chunks(self.max_batch as usize) {
            let mut lines = Vec::new();

            for measurement in chunk {
                lines.push(measurement.to_string());
            }

            let mut query = HashMap::new();
            query.insert("db", self.credentials.database.to_string());

            if let Some(ref precision) = precision {
                query.insert("precision", precision.to_string());
            }

            let request = Request {
                url: &*{ self.host.to_owned() + "/write" },
                method: Method::POST,
                auth: Some(Auth {
                    username: self.credentials.username,
                    password: self.credentials.password,
                }),
                query: Some(query),
                body: Some(lines.join("\n")),
            };

            let resp = self
                .hurl
                .request(request)
                .await
                .map_err(ClientError::Communication)?;
            match resp.status {
                204 => (),
                200 => return Err(ClientError::CouldNotComplete(resp.to_string())),
                400 => return Err(ClientError::Syntax(resp.to_string())),
                _ => {
                    return Err(ClientError::Unexpected(format!(
                        "Unexpected response. Status: {}; Body: \"{}\"",
                        resp.status,
                        resp.to_string()
                    )))
                }
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::InfluxClient;
    use crate::{
        client::{Client, Credentials, Precision},
        hurl::{Hurl, Request, Response},
        point::Point,
    };
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockHurl<F>
    where
        F: Fn() -> Result<Response, String> + Send + Sync,
    {
        request_count: AtomicUsize,
        result: F,
    }

    impl<F> MockHurl<F>
    where
        F: Fn() -> Result<Response, String> + Send + Sync,
    {
        fn new(result: F) -> Self {
            Self {
                request_count: AtomicUsize::new(0),
                result,
            }
        }
    }

    #[async_trait]
    impl<F> Hurl for MockHurl<F>
    where
        F: Fn() -> Result<Response, String> + Send + Sync,
    {
        async fn request(&self, req: Request<'_>) -> Result<Response, String> {
            println!("sending: {:?}", req);
            self.request_count.fetch_add(1, Ordering::SeqCst);
            let ref f = self.result;
            f()
        }
    }

    fn client_with_response<'a>(
        response: Result<Response, String>,
        host: &'a str,
    ) -> InfluxClient<'a> {
        let mut client = InfluxClient::new(
            Credentials {
                username: "gobwas",
                password: "1234",
                database: "test",
            },
            host,
        );
        client.set_hurl(Box::new(MockHurl::new(|| response)));
        client
    }

    #[tokio::test]
    async fn test_write_one() {
        client_with_response(
            Ok(Response {
                status: 204,
                body: "Ok".to_string(),
            }),
            "http://localhost:8086",
        )
        .write_one(Point::new("key"), Some(Precision::Nanoseconds))
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_many() {
        client_with_response(
            Ok(Response {
                status: 204,
                body: "Ok".to_string(),
            }),
            "http://localhost:8086",
        )
        .write_many(&[Point::new("key")], Some(Precision::Nanoseconds))
        .await
        .unwrap();
    }
}
