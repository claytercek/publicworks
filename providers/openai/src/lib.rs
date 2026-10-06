//! Opt-in, non-streaming OpenAI Responses provider for a Tokio host.
//!
//! Only text and local function calls are supported, not reasoning models.
//! Credentials and transport configuration stay in host memory. Model inputs,
//! outputs and usage remain durable application data, not scrubbed secrets.
use futures_util::future::{Either, select};
use publicworks_agent::{Cancellation, Model, ModelError, ModelFuture, ModelRequest};
use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue},
};
use std::{fmt, net::IpAddr, time::Duration};

mod wire;

/// Transport/security limits only; model semantics come from `ModelRequest`.
#[derive(Clone)]
pub struct Config {
    pub endpoint: String,
    pub timeout: Duration,
    pub max_response_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: "https://api.openai.com/v1/responses".into(),
            timeout: Duration::from_secs(120),
            max_response_bytes: 8 * 1024 * 1024,
        }
    }
}
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("endpoint", &"[redacted]")
            .field("timeout", &self.timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

/// Constant diagnostics deliberately retain no URL, credential or client error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    InvalidApiKey,
    InvalidEndpoint,
    InvalidLimits,
    ClientInitialization,
}
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidApiKey => "Invalid OpenAI API key",
            Self::InvalidEndpoint => "Invalid OpenAI endpoint",
            Self::InvalidLimits => "Invalid OpenAI transport limits",
            Self::ClientInitialization => "OpenAI HTTP client initialization failed",
        })
    }
}
impl std::error::Error for ConfigError {}

/// Reusable asynchronous client. Poll model futures inside a Tokio runtime.
#[derive(Clone)]
pub struct OpenAiResponses {
    client: Client,
    endpoint: Url,
    authorization: HeaderValue,
    max_response_bytes: usize,
}
impl fmt::Debug for OpenAiResponses {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiResponses").finish_non_exhaustive()
    }
}
impl OpenAiResponses {
    pub fn new(api_key: impl Into<String>) -> Result<Self, ConfigError> {
        Self::with_config(api_key, Config::default())
    }

    pub fn with_config(api_key: impl Into<String>, config: Config) -> Result<Self, ConfigError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(ConfigError::InvalidApiKey);
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {api_key}"))
            .map_err(|_| ConfigError::InvalidApiKey)?;
        authorization.set_sensitive(true);
        // Require an unambiguous raw authority before WHATWG URL parsing:
        // it otherwise repairs extra slashes, strips controls/empty userinfo,
        // and treats backslashes as slashes for HTTP(S).
        if config
            .endpoint
            .chars()
            .any(|c| c.is_control() || c.is_whitespace() || c == '\\')
        {
            return Err(ConfigError::InvalidEndpoint);
        }
        let (scheme, rest) = config
            .endpoint
            .split_once("://")
            .ok_or(ConfigError::InvalidEndpoint)?;
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        if !(scheme.eq_ignore_ascii_case("https") || scheme.eq_ignore_ascii_case("http"))
            || authority.is_empty()
            || authority.contains('@')
        {
            return Err(ConfigError::InvalidEndpoint);
        }
        let endpoint = Url::parse(&config.endpoint).map_err(|_| ConfigError::InvalidEndpoint)?;
        let numeric_loopback = endpoint
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        if endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !(endpoint.scheme() == "https" || (endpoint.scheme() == "http" && numeric_loopback))
        {
            return Err(ConfigError::InvalidEndpoint);
        }
        if config.timeout.is_zero()
            || config.max_response_bytes == 0
            || std::time::Instant::now()
                .checked_add(config.timeout)
                .is_none()
        {
            return Err(ConfigError::InvalidLimits);
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .timeout(config.timeout)
            .build()
            .map_err(|_| ConfigError::ClientInitialization)?;
        Ok(Self {
            client,
            endpoint,
            authorization,
            max_response_bytes: config.max_response_bytes,
        })
    }

    async fn send(
        &self,
        request: ModelRequest,
        cancellation: &Cancellation,
    ) -> Result<publicworks_agent::ModelResponse, ModelError> {
        if cancellation.is_cancelled() {
            return Err(error("OpenAI request cancelled"));
        }
        let body = wire::encode_request(&request)?;
        if cancellation.is_cancelled() {
            return Err(error("OpenAI request cancelled"));
        }
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .header(AUTHORIZATION, self.authorization.clone())
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(transport_error)?;
        if !response.status().is_success() {
            // Neither server error text nor request IDs are safe diagnostics.
            return Err(error(&format!(
                "OpenAI HTTP status {}",
                response.status().as_u16()
            )));
        }
        if response
            .content_length()
            .is_some_and(|n| n > self.max_response_bytes as u64)
        {
            return Err(error("OpenAI response body exceeds limit"));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
            let length = body
                .len()
                .checked_add(chunk.len())
                .filter(|length| *length <= self.max_response_bytes)
                .ok_or_else(|| error("OpenAI response body exceeds limit"))?;
            body.reserve(length - body.len());
            body.extend_from_slice(&chunk);
        }
        if cancellation.is_cancelled() {
            return Err(error("OpenAI request cancelled"));
        }
        wire::decode_response(&body)
    }
}
impl Model for OpenAiResponses {
    fn complete(&self, request: ModelRequest, cancellation: Cancellation) -> ModelFuture {
        let provider = self.clone();
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(error("OpenAI request cancelled"));
            }
            // Cancellation is polled first, and stays raced through headers/body.
            let cancelled = Box::pin(cancellation.cancelled());
            let send = Box::pin(provider.send(request, &cancellation));
            match select(cancelled, send).await {
                Either::Left(_) => Err(error("OpenAI request cancelled")),
                Either::Right((result, _)) => result,
            }
        })
    }
}
fn error(message: &str) -> ModelError {
    ModelError {
        message: message.into(),
        partial_response: None,
        usage: None,
    }
}
fn transport_error(cause: reqwest::Error) -> ModelError {
    error(if cause.is_timeout() {
        "OpenAI request timed out"
    } else if cause.is_connect() {
        "OpenAI connection failed"
    } else if cause.is_body() || cause.is_decode() {
        "OpenAI response body failed"
    } else {
        "OpenAI transport failed"
    })
}

#[cfg(test)]
mod tests;
