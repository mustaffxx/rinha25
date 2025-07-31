use crate::models::{AppState, PaymentMetrics, PaymentRequest, PaymentSummary, SummaryQuery};
use crate::redis_helpers::{acquire_redis_lock, release_redis_lock};
use actix_web::{HttpResponse, Result, web};
use chrono::Utc;
use deadpool_redis::redis::AsyncCommands;
use serde_json::json;

#[actix_web::post("/payments")]
pub async fn create_payment(
    payment: web::Json<PaymentRequest>,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    let state = data.get_ref();
    let sender = &state.payment_sender;

    let mut payment_data = payment.into_inner();
    payment_data.requested_at = Some(Utc::now());

    if let Err(e) = sender.send(payment_data).await {
        eprintln!("Failed to send payment: {}", e);
        return Err(actix_web::error::ErrorInternalServerError(
            "Failed to process payment",
        ));
    }

    Ok(HttpResponse::Ok().json(json!({"status": "accepted"})))
}

#[actix_web::get("/payments-summary")]
pub async fn get_payment_summary(
    query: web::Query<SummaryQuery>,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    const LOCK_KEY: &str = "payments_summary_lock";
    const LOCK_TTL: usize = 10;

    if !acquire_redis_lock(&data.redis_pool, LOCK_KEY, LOCK_TTL).await {
        return Err(actix_web::error::ErrorTooManyRequests(
            "Summary in progress, try again later",
        ));
    }

    let redis_pool = &data.redis_pool;
    let mut conn = redis_pool.get().await.map_err(|e| {
        eprintln!("Failed to get Redis connection for summary: {}", e);
        actix_web::error::ErrorInternalServerError("Redis error")
    })?;

    let from_ts = query
        .from
        .map_or(f64::MIN, |dt| dt.timestamp_millis() as f64);

    let to_ts = query.to.map_or(f64::MAX, |dt| dt.timestamp_millis() as f64);

    let default_metrics = get_metrics(&mut conn, "default", from_ts, to_ts)
        .await
        .unwrap_or_default();

    let fallback_metrics = get_metrics(&mut conn, "fallback", from_ts, to_ts)
        .await
        .unwrap_or_default();

    let summary = PaymentSummary {
        default: default_metrics,
        fallback: fallback_metrics,
    };

    release_redis_lock(&data.redis_pool, LOCK_KEY).await;

    Ok(HttpResponse::Ok().json(summary))
}

async fn get_metrics(
    conn: &mut deadpool_redis::Connection,
    processor: &str,
    from_ts: f64,
    to_ts: f64,
) -> deadpool_redis::redis::RedisResult<PaymentMetrics> {
    let zset_key = format!("summary:{}:history", processor);
    let ids: Vec<String> = conn.zrangebyscore(&zset_key, from_ts, to_ts).await?;
    if ids.is_empty() {
        return Ok(PaymentMetrics::default());
    }

    let hset_key = format!("summary:{}:data", processor);
    let amounts: Vec<f64> = conn.hmget(&hset_key, &ids).await?;
    let total_requests = amounts.len() as u64;
    let total_amount = (amounts.iter().sum::<f64>() * 100.0).round() / 100.0;
    Ok(PaymentMetrics {
        total_requests,
        total_amount,
    })
}
