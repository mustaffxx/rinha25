use crate::health::get_process_payment;
use crate::models::{HealthResponse, PaymentRequest};
use crate::redis_helpers::is_redis_locked;
use chrono::Utc;
use deadpool_redis::Pool as RedisPool;
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::redis::pipe;
use rand::{Rng, thread_rng};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

pub async fn payment_worker(
    worker_id: usize,
    http_client: reqwest::Client,
    redis_pool: RedisPool,
    health_data: Arc<Mutex<HashMap<String, HealthResponse>>>,
) {
    println!("Worker {} started", worker_id);

    loop {
        let mut redis_conn = match redis_pool.get().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Worker {} failed to get Redis connection: {}", worker_id, e);
                continue;
            }
        };

        let payment_data: Option<PaymentRequest> = redis_conn
            .blpop::<_, (String, Vec<u8>)>("payments_queue", 0f64)
            .await
            .ok()
            .and_then(|(_, payload)| serde_json::from_slice(&payload).ok());

        if payment_data.is_none() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        if let Some(mut payment_data) = payment_data {
            let lock_key = "payments_summary_lock";
            while is_redis_locked(&redis_pool, lock_key).await {
                let jitter = thread_rng().gen_range(0..50);
                tokio::time::sleep(std::time::Duration::from_millis(50 + jitter)).await;
            }

            println!(
                "Worker {} processing payment: {}",
                worker_id, payment_data.correlation_id
            );

            loop {
                let successful_processor =
                    get_process_payment(&http_client, &health_data, &payment_data).await;

                if let Some(processor) = successful_processor {
                    let payment_url = match processor {
                        "default" => "http://payment-processor-default:8080/payments",
                        "fallback" => "http://payment-processor-fallback:8080/payments",
                        _ => {
                            eprintln!("Worker {} unknown processor: {}", worker_id, processor);
                            continue;
                        }
                    };

                    payment_data.requested_at = Some(Utc::now());
                    let processor_start = std::time::Instant::now();
                    let send_result = send_payment(&http_client, payment_url, &payment_data).await;
                    let processor_duration = processor_start.elapsed();
                    let processor_duration_ms = processor_duration.as_millis() as u64;

                    match &send_result {
                        Ok(_) => {
                            println!(
                                "Worker {}: processor request for {} via {} succeeded in {:?}",
                                worker_id,
                                payment_data.correlation_id,
                                processor,
                                processor_duration
                            );

                            let mut map = health_data.lock().await;
                            if let Some(health) = map.get_mut(processor) {
                                if health.failing {
                                    health.failing = false;
                                    health.min_response_time = processor_duration_ms;
                                    println!(
                                        "Worker {}: processor {} marked healthy after successful payment",
                                        worker_id, processor
                                    );
                                }
                            }
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

                            let mut map = health_data.lock().await;
                            if let Some(health) = map.get_mut(processor) {
                                if !health.failing {
                                    health.failing = true;
                                    health.min_response_time = processor_duration_ms;
                                    println!(
                                        "Worker {}: processor {} marked unhealthy after failed payment",
                                        worker_id, processor
                                    );
                                }
                            }
                        }
                    }

                    if send_result.is_err() {
                        continue;
                    }

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
