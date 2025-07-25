use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Utc};
use deadpool_redis::Config as RedisConfig;
use deadpool_redis::Pool as RedisPool;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;

pub type DbPool = Pool<PostgresConnectionManager<NoTls>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PaymentMetrics {
    #[serde(rename = "totalRequests")]
    pub total_requests: u64,
    #[serde(rename = "totalAmount")]
    pub total_amount: f64,
}

impl Default for PaymentMetrics {
    fn default() -> Self {
        PaymentMetrics {
            total_requests: 0,
            total_amount: 0.0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PaymentSummary {
    pub default: PaymentMetrics,
    pub fallback: PaymentMetrics,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SummaryQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PaymentRequest {
    #[serde(rename = "correlationId")]
    pub correlation_id: String,
    pub amount: f64,
    #[serde(rename = "requestedAt")]
    pub requested_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HealthResponse {
    pub failing: bool,
    #[serde(rename = "minResponseTime")]
    pub min_response_time: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub http_client: reqwest::Client,
    pub redis_pool: RedisPool,
    pub db: DbPool,
    pub payment_queue: mpsc::UnboundedSender<PaymentRequest>,
    pub health_data: Arc<Mutex<HashMap<String, HealthResponse>>>,
}

impl AppState {
    pub fn new(
        http_client: reqwest::Client,
        redis_pool: RedisPool,
        db: DbPool,
        payment_queue: mpsc::UnboundedSender<PaymentRequest>,
        health_data: Arc<Mutex<HashMap<String, HealthResponse>>>,
    ) -> Self {
        Self {
            http_client,
            redis_pool,
            db,
            payment_queue,
            health_data,
        }
    }
}

pub fn create_pg_manager(
    database_url: &str,
) -> Result<PostgresConnectionManager<NoTls>, std::io::Error> {
    PostgresConnectionManager::new_from_stringlike(database_url, NoTls)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub async fn create_db_pool(
    manager: PostgresConnectionManager<NoTls>,
) -> Result<DbPool, std::io::Error> {
    Pool::builder()
        .build(manager)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e))
}

pub fn create_redis_pool(redis_url: &str) -> Result<RedisPool, std::io::Error> {
    let mut cfg = RedisConfig::default();
    cfg.url = Some(redis_url.to_string());
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e))
}
