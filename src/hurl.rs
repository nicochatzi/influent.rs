use async_trait::async_trait;
use reqwest::Client as ReqwestClient;
use reqwest::Method as ReqwestMethod;
use std::collections::HashMap;
use url::Url;

#[async_trait]
pub trait Hurl {
    async fn request(&self, req: Request<'_>) -> Result<Response, String>;
}

#[derive(Debug)]
pub struct Request<'a> {
    pub url: &'a str,
    pub method: Method,
    pub auth: Option<Auth<'a>>,
    pub query: Option<HashMap<&'a str, String>>,
    pub body: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub body: String,
}

impl ToString for Response {
    fn to_string(&self) -> String {
        self.body.clone()
    }
}

#[derive(Debug)]
pub enum Method {
    POST,
    GET,
}

#[derive(Debug)]
pub struct Auth<'a> {
    pub username: &'a str,
    pub password: &'a str,
}

#[derive(Default)]
pub struct ReqwestHurl;

#[async_trait]
impl Hurl for ReqwestHurl {
    async fn request(&self, req: Request<'_>) -> Result<Response, String> {
        let client = ReqwestClient::new();

        // map request method to the hyper's
        let method = match req.method {
            Method::POST => ReqwestMethod::POST,
            Method::GET => ReqwestMethod::GET,
        };

        let mut url = Url::parse(req.url).map_err(|e| format!("could not parse url: {:?}", e))?;

        // if request has query
        if let Some(ref query) = req.query {
            // if any existing pairs
            let existing: Vec<(String, String)> = url
                .query_pairs()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect();

            // final pairs
            let mut pairs: Vec<(&str, &str)> = Vec::new();

            // add first existing
            for pair in &existing {
                pairs.push((&pair.0, &pair.1));
            }

            // add given query to the pairs
            for (key, val) in query.iter() {
                pairs.push((key, val));
            }

            // set new pairs
            url.query_pairs_mut()
                .clear()
                .extend_pairs(pairs.iter().map(|&(k, v)| (&k[..], &v[..])));
        }

        // create query
        let mut builder = client
            .request(method, url.as_str())
            .body(req.body.unwrap_or_else(|| "".to_owned()));

        // if request need to be authorized
        if let Some(auth) = req.auth {
            builder = builder.basic_auth(auth.username, Some(auth.password));
        }

        let request = builder.build().unwrap();

        let resp = client.execute(request).await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let body = resp.text().await.map_err(|e| e.to_string())?;

        Ok(Response { status, body })
    }
}
