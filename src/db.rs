use crate::models::{DbPool, PaymentMetrics, PaymentRequest, PaymentSummary};
use bb8_postgres::PostgresConnectionManager;
use deadpool_redis::Pool as RedisPool;
use deadpool_redis::redis::AsyncCommands;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

pub async fn payment_exists(db: &DbPool, redis_pool: &RedisPool, correlation_id: &str) -> bool {
    let cache_key = format!("payment_exists:{}", correlation_id);
    if let Ok(mut conn) = redis_pool.get().await {
        if let Ok(Some(1)) = conn.get::<_, Option<u8>>(&cache_key).await {
            return true;
        }
    }
    let found = match db.get().await {
        Ok(conn) => {
            let row = conn
                .query_one(
                    "SELECT 1 FROM payment_events WHERE correlation_id = $1 LIMIT 1",
                    &[&correlation_id],
                )
                .await;
            row.is_ok()
        }
        Err(e) => {
            eprintln!(
                "Failed to get database connection for duplicate check: {}",
                e
            );
            false
        }
    };
    if found {
        if let Ok(mut conn) = redis_pool.get().await {
            let _: Result<(), _> = conn.set_ex(&cache_key, 1u8, 60 * 10).await;
        }
    }
    found
}

pub async fn record_payment_with_conn(
    conn: &bb8::PooledConnection<'_, PostgresConnectionManager<tokio_postgres::NoTls>>,
    processor: &str,
    payment: &PaymentRequest,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let amount_decimal = Decimal::from_f64(payment.amount).unwrap_or(Decimal::ZERO);
    match conn
        .execute(
            "INSERT INTO payment_events (correlation_id, processor, amount, requested_at) VALUES ($1, $2, $3, $4)",
            &[&payment.correlation_id, &processor, &amount_decimal, &payment.requested_at],
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Failed to insert payment event: {}", e);
            Err(e.into())
        }
    }
}

pub async fn fetch_payment_summary(
    db: &DbPool,
    from: Option<chrono::DateTime<chrono::Utc>>,
    to: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<PaymentSummary, Box<dyn std::error::Error + Send + Sync>> {
    let conn = db.get().await?;
    let query = "
        SELECT 
            processor, 
            COUNT(*) AS total_requests, 
            COALESCE(SUM(amount), 0) AS total_amount 
        FROM payment_events 
        WHERE ($1::timestamptz IS NULL OR requested_at >= $1)
          AND ($2::timestamptz IS NULL OR requested_at <= $2)
        GROUP BY processor";
    let rows = conn.query(query, &[&from, &to]).await?;
    let mut summary = PaymentSummary {
        default: PaymentMetrics {
            total_requests: 0,
            total_amount: 0.0,
        },
        fallback: PaymentMetrics {
            total_requests: 0,
            total_amount: 0.0,
        },
    };
    for row in rows {
        let processor: String = row.get(0);
        let requests: i64 = row.get(1);
        let amount: Decimal = row.get(2);
        match processor.as_str() {
            "default" => {
                summary.default.total_requests = requests as u64;
                summary.default.total_amount = amount.to_string().parse().unwrap_or(0.0);
            }
            "fallback" => {
                summary.fallback.total_requests = requests as u64;
                summary.fallback.total_amount = amount.to_string().parse().unwrap_or(0.0);
            }
            _ => {}
        }
    }
    Ok(summary)
}
