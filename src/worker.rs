use crate::models::PaymentRequest;
use crate::redis_helpers::is_redis_locked;
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
            let lock_key = "payments_summary_lock";
            while is_redis_locked(&redis_pool, lock_key).await {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            println!(
                "Worker {} processing payment: {}",
                worker_id, payment_data.correlation_id
            );

            loop {
                let processor = "default";
                let payment_url = "http://payment-processor-default:8080/payments";

                payment_data.requested_at = Some(Utc::now());
                let processor_start = std::time::Instant::now();
                let send_result = send_payment(&http_client, payment_url, &payment_data).await;
                let processor_duration = processor_start.elapsed();

                match &send_result {
                    Ok(_) => {
                        println!(
                            "Worker {}: processor request for {} via {} succeeded in {:?}",
                            worker_id, payment_data.correlation_id, processor, processor_duration
                        );

                        let redis_store_start = std::time::Instant::now();
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

                        let dur = redis_store_start.elapsed();
                        match pipe_result {
                            Ok(_) => println!(
                                "Worker {}: stored payment {} (amount: {}) for {} in Redis in {:?}",
                                worker_id, id, amount, processor, dur
                            ),
                            Err(e) => eprintln!(
                                "Worker {}: failed to store payment {} for {} in Redis in {:?}: {}",
                                worker_id, id, processor, dur, e
                            ),
                        }

                        break;
                    }
                    Err(e) => {
                        println!(
                            "Worker {}: processor request for {} via {} failed in {:?}: {}",
                            worker_id,
                            payment_data.correlation_id,
                            processor,
                            processor_duration,
                            e
                        );

                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
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
        println!("Payment processed successfully: {}", payment.correlation_id);
        Ok(())
    } else {
        Err(format!("Payment failed with status: {}", response.status()).into())
    }
}
