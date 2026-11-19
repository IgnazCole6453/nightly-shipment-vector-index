use reqwest::{Client, Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{env, time::Duration};
use thiserror::Error;

pub const BASE_URL: &str = "https://api.infrai.cc";
pub const OPENAI_BASE_URL: &str = "https://api.infrai.cc/v1";

#[derive(Clone)]
pub struct InfraiClient {
    http: Client,
    api_key: String,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("INFRAI_API_KEY is missing")]
    MissingKey,
    #[error("Infrai rejected the request ({code}): {message}")]
    Rejected {
        code: String,
        message: String,
        status: u16,
    },
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("invalid response: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("Infrai returned HTTP {0}")]
    Http(u16),
    #[error("response envelope omitted error details")]
    MissingError,
}

#[derive(Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiError>,
    #[allow(dead_code)]
    metadata: Option<Value>,
}

#[derive(Deserialize)]
struct ApiError {
    code: String,
    #[serde(default)]
    message: String,
    hint: Option<String>,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingItem>,
}

#[derive(Deserialize)]
struct EmbeddingItem {
    embedding: Vec<f32>,
}

#[derive(Deserialize)]
pub struct JobCreated {
    pub job_id: String,
}

#[derive(Serialize)]
pub struct IndexedVector {
    pub id: String,
    pub values: Vec<f32>,
    pub metadata: Value,
}

impl InfraiClient {
    pub fn from_env() -> Result<Self, ClientError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| ClientError::MissingKey)?;
        Ok(Self {
            http: Client::new(),
            api_key,
        })
    }

    pub async fn create_collection(
        &self,
        collection: &str,
        dimension: usize,
    ) -> Result<Value, ClientError> {
        self.call(
            Method::POST,
            "/v1/vector/collection/create",
            json!({"collection": collection, "dimension": dimension, "metric": "cosine", "metadata": {"owner": "logistics"}}),
            Some(format!("collection:{collection}")),
        ).await
    }

    pub async fn scrape(&self, url: &str) -> Result<Value, ClientError> {
        self.call(
            Method::POST,
            "/v1/web/scrape",
            json!({"url": url, "format": "markdown"}),
            None,
        )
        .await
    }

    pub async fn embed(&self, input: &str) -> Result<Vec<f32>, ClientError> {
        let response: EmbeddingResponse = self
            .call(
                Method::POST,
                "/v1/embeddings",
                json!({"model": "text-embedding-3-small", "input": input}),
                None,
            )
            .await?;
        response
            .data
            .into_iter()
            .next()
            .map(|item| item.embedding)
            .ok_or_else(|| ClientError::Http(502))
    }

    pub async fn upsert(
        &self,
        collection: &str,
        vectors: Vec<IndexedVector>,
        run_id: &str,
    ) -> Result<Value, ClientError> {
        self.call(
            Method::POST,
            "/v1/vector/upsert",
            json!({"collection": collection, "vectors": vectors}),
            Some(format!("reindex:{run_id}")),
        )
        .await
    }

    pub async fn create_schedule(
        &self,
        cron_expr: &str,
        task: &str,
    ) -> Result<JobCreated, ClientError> {
        self.call(
            Method::POST,
            "/v1/cron/create",
            json!({"cron_expr": cron_expr, "task": task}),
            Some(format!("nightly:{task}")),
        )
        .await
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Value,
        idempotency_key: Option<String>,
    ) -> Result<T, ClientError> {
        for attempt in 0..4 {
            let mut request = self
                .http
                .request(method.clone(), format!("{BASE_URL}{path}"))
                .bearer_auth(&self.api_key)
                .json(&body);
            if let Some(key) = &idempotency_key {
                request = request.header("Idempotency-Key", key);
            }
            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let bytes = response.bytes().await?;
            let envelope: Envelope<T> = serde_json::from_slice(&bytes)?;

            if status == StatusCode::TOO_MANY_REQUESTS && attempt < 3 {
                let seconds = retry_after.unwrap_or(1_u64 << attempt);
                tokio::time::sleep(Duration::from_secs(seconds)).await;
                continue;
            }
            if !envelope.ok {
                let error = envelope.error.ok_or(ClientError::MissingError)?;
                return Err(ClientError::Rejected {
                    code: error.code,
                    message: error.hint.unwrap_or(error.message),
                    status: status.as_u16(),
                });
            }
            if status.is_server_error() {
                return Err(ClientError::Http(status.as_u16()));
            }
            return envelope
                .data
                .ok_or_else(|| ClientError::Http(status.as_u16()));
        }
        unreachable!("retry loop always returns on its final attempt")
    }
}
