use crate::models::PaymentRequest;
use chrono::Utc;
use deadpool_redis::Pool as RedisPool;
use deadpool_redis::redis::pipe;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

pub async fn payment_worker(
    worker_id: usize,
    http_client: reqwest::Client,
    redis_pool: RedisPool,
    payment_receiver: Arc<Mutex<mpsc::Receiver<PaymentRequest>>>,
) {
    println!("Worker {} started", worker_id);

    let processor = "default";
    let payment_url = "http://payment-processor-default:8080/payments";

    let mut redis_conn = match redis_pool.get().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Worker {} failed to get Redis connection: {}", worker_id, e);
            return;
        }
    };

    loop {
        let payment_data = payment_receiver.lock().await.recv().await;
        if payment_data.is_none() {
            continue;
        }

        if let Some(mut payment_data) = payment_data {
            loop {
                payment_data.requested_at = Some(Utc::now());
                let send_result = send_payment(&http_client, payment_url, &payment_data).await;
                if send_result.is_ok() {
                    let id = &payment_data.correlation_id;
                    let amount = payment_data.amount;

                    let timestamp_ms = payment_data
                        .requested_at
                        .map(|dt| dt.timestamp_millis() as f64)
                        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as f64);

                    let pipe_result: deadpool_redis::redis::RedisResult<()> = pipe()
                        .atomic()
                        .hset(format!("summary:{}:data", processor), id, amount)
                        .zadd(format!("summary:{}:history", processor), id, timestamp_ms)
                        .query_async(&mut redis_conn)
                        .await;

                    match pipe_result {
                        Ok(_) => (),
                        Err(e) => eprintln!(
                            "Worker {}: failed to store payment {} for {} in Redis: {}",
                            worker_id, id, processor, e
                        ),
                    }

                    break;
                }
            }
        }
    }
}

async fn send_payment(
    client: &reqwest::Client,
    url: &str,
    payment: &PaymentRequest,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let response = client.post(url).json(payment).send().await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("Payment failed with status: {}", response.status()).into())
    }
}
