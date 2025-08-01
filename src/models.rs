use chrono::{DateTime, Utc};
use deadpool_redis::Pool as RedisPool;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaymentMetrics {
    pub total_requests: u64,
    pub total_amount: f64,
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
#[serde(rename_all = "camelCase")]
pub struct PaymentRequest {
    pub correlation_id: String,
    pub amount: f64,
    pub requested_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HealthResponse {
    pub failing: bool,
    pub min_response_time: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub http_client: reqwest::Client,
    pub redis_pool: RedisPool,
    pub payment_sender: mpsc::Sender<PaymentRequest>,
}

impl AppState {
    pub fn new(
        http_client: reqwest::Client,
        redis_pool: RedisPool,
        payment_sender: mpsc::Sender<PaymentRequest>,
    ) -> Self {
        Self {
            http_client,
            redis_pool,
            payment_sender,
        }
    }
}
