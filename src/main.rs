use actix_web::{App, HttpResponse, HttpServer, Result, web};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Utc};
use rand::{Rng, thread_rng};
use redis::pipe;
use redis::{AsyncCommands, Client as RedisClient};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;

type DbPool = Pool<PostgresConnectionManager<NoTls>>;

#[derive(Serialize, Deserialize)]
struct PaymentMetrics {
    #[serde(rename = "totalRequests")]
    total_requests: u64,
    #[serde(rename = "totalAmount")]
    total_amount: f64,
}

#[derive(Serialize, Deserialize)]
struct PaymentSummary {
    default: PaymentMetrics,
    fallback: PaymentMetrics,
}

#[derive(Deserialize)]
struct SummaryQuery {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize)]
struct PaymentRequest {
    #[serde(rename = "correlationId")]
    correlation_id: String,
    amount: f64,
    #[serde(rename = "requestedAt")]
    requested_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone)]
struct HealthResponse {
    failing: bool,
    #[serde(rename = "minResponseTime")]
    min_response_time: u64,
}

#[derive(Clone)]
struct AppState {
    http_client: reqwest::Client,
    redis_client: RedisClient,
    db: DbPool,
    payment_queue: mpsc::UnboundedSender<PaymentRequest>,
    health_data: Arc<Mutex<HashMap<String, HealthResponse>>>,
}

#[actix_web::post("/payments")]
async fn create_payment(
    payment: web::Json<PaymentRequest>,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    let mut payment_data = payment.into_inner();
    payment_data.requested_at = Some(Utc::now());
    let state = data.get_ref();

    // Serialize, check cache, and push to Redis in a background task for speed
    let redis_client = state.redis_client.clone();
    let correlation_id = payment_data.correlation_id.clone();
    tokio::spawn(async move {
        if let Ok(mut conn) = redis_client.get_async_connection().await {
            // Check cache for duplicate
            let cache_key = format!("payment_exists:{}", correlation_id);
            if let Ok(Some(1)) = conn.get::<_, Option<u8>>(&cache_key).await {
                // Already exists, do not enqueue
                return;
            }
            if let Ok(payload) = serde_json::to_vec(&payment_data) {
                let _ = conn.rpush::<_, _, ()>("payments_queue", payload).await;
                // After enqueue, set cache key to mark as existing (10 min)
                let _: Result<(), _> = conn.set_ex(&cache_key, 1u8, 60 * 10).await;
            }
        }
    });

    Ok(HttpResponse::Ok().json(serde_json::json!({"status": "accepted"})))
}

#[actix_web::get("/payments-summary")]
async fn get_payment_summary(
    query: web::Query<SummaryQuery>,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    // Acquire distributed lock before running summary
    let lock_key = "payments_summary_lock";
    let lock_ttl = 10; // seconds
    if !acquire_redis_lock(&data.redis_client, lock_key, lock_ttl).await {
        return Err(actix_web::error::ErrorTooManyRequests(
            "Summary in progress, try again later",
        ));
    }

    let redis_client = &data.redis_client;
    let from_ts = query
        .from
        .map(|dt| dt.timestamp_millis() as f64)
        .unwrap_or(f64::MIN);
    let to_ts = query
        .to
        .map(|dt| dt.timestamp_millis() as f64)
        .unwrap_or(f64::MAX);
    let mut conn = redis_client.get_async_connection().await.map_err(|e| {
        eprintln!("Failed to get Redis connection for summary: {}", e);
        actix_web::error::ErrorInternalServerError("Redis error")
    })?;

    async fn get_metrics(
        conn: &mut redis::aio::Connection,
        processor: &str,
        from_ts: f64,
        to_ts: f64,
    ) -> redis::RedisResult<PaymentMetrics> {
        let zset_key = format!("summary:{}:history", processor);
        let hset_key = format!("summary:{}:data", processor);
        // Get all correlation_ids in the time range
        let ids: Vec<String> = conn.zrangebyscore(&zset_key, from_ts, to_ts).await?;
        if ids.is_empty() {
            return Ok(PaymentMetrics {
                total_requests: 0,
                total_amount: 0.0,
            });
        }
        // Get all amounts for these ids
        let amounts: Vec<f64> = conn.hget(&hset_key, &ids).await?;
        let total_requests = amounts.len() as u64;
        let total_amount = (amounts.iter().copied().sum::<f64>() * 100.0).round() / 100.0;
        Ok(PaymentMetrics {
            total_requests,
            total_amount,
        })
    }

    let default_metrics = get_metrics(&mut conn, "default", from_ts, to_ts)
        .await
        .unwrap_or(PaymentMetrics {
            total_requests: 0,
            total_amount: 0.0,
        });
    let fallback_metrics = get_metrics(&mut conn, "fallback", from_ts, to_ts)
        .await
        .unwrap_or(PaymentMetrics {
            total_requests: 0,
            total_amount: 0.0,
        });

    let summary = PaymentSummary {
        default: default_metrics,
        fallback: fallback_metrics,
    };

    release_redis_lock(&data.redis_client, lock_key).await;
    Ok(HttpResponse::Ok().json(summary))
}

// Checks if a payment with the given correlation_id already exists in the cache or database
async fn payment_exists(db: &DbPool, redis: &RedisClient, correlation_id: &str) -> bool {
    let cache_key = format!("payment_exists:{}", correlation_id);
    // Check Redis cache first
    if let Ok(mut conn) = redis.get_async_connection().await {
        if let Ok(Some(1)) = conn.get::<_, Option<u8>>(&cache_key).await {
            // Found in cache
            return true;
        }
    }

    // Not found in cache, check DB
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
            false // fallback: process if can't check
        }
    };

    // If found in DB, set cache for future
    if found {
        if let Ok(mut conn) = redis.get_async_connection().await {
            let _: Result<(), _> = conn.set_ex(&cache_key, 1u8, 60 * 10).await; // cache for 10 min
        }
    }

    found
}

async fn record_payment_with_conn(
    conn: &bb8::PooledConnection<'_, PostgresConnectionManager<NoTls>>,
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

async fn fetch_payment_summary(
    db: &DbPool,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
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

async fn get_processor_healthy(
    health_data: &Arc<Mutex<HashMap<String, HealthResponse>>>,
    processor: &str,
) -> Option<HealthResponse> {
    let map = health_data.lock().await;
    map.get(processor).cloned()
}

fn health_checker(
    health_data: Arc<Mutex<HashMap<String, HealthResponse>>>,
    client: reqwest::Client,
) {
    // Default processor health check
    let health_data_default = health_data.clone();
    let client_default = client.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        loop {
            interval.tick().await;
            if let Ok(default) = check_processor_health(
                &client_default,
                "http://payment-processor-default:8080/payments/service-health",
            )
            .await
            {
                let mut map = health_data_default.lock().await;
                map.insert("default".to_string(), default);
            }
        }
    });

    // Fallback processor health check
    let health_data_fallback = health_data.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        loop {
            interval.tick().await;
            if let Ok(fallback) = check_processor_health(
                &client,
                "http://payment-processor-fallback:8080/payments/service-health",
            )
            .await
            {
                let mut map = health_data_fallback.lock().await;
                map.insert("fallback".to_string(), fallback);
            }
        }
    });
}

async fn check_processor_health(
    client: &reqwest::Client,
    url: &str,
) -> Result<HealthResponse, Box<dyn std::error::Error + Send + Sync>> {
    let response = client
        .get(url)
        .timeout(Duration::from_millis(200))
        .send()
        .await?;

    Ok(response.json().await?)
}

async fn get_process_payment(
    _http_client: &reqwest::Client,
    health_data: &Arc<Mutex<HashMap<String, HealthResponse>>>,
    _payment_data: &PaymentRequest,
) -> Option<&'static str> {
    let default_healthy = get_processor_healthy(health_data, "default").await;
    let fallback_healthy = get_processor_healthy(health_data, "fallback").await;

    match (default_healthy, fallback_healthy) {
        (Some(default), Some(fallback)) => match (default.failing, fallback.failing) {
            (true, true) => {
                eprintln!("Both processors are unhealthy");
                None
            }
            (false, false) => {
                if default.min_response_time <= fallback.min_response_time {
                    Some("default")
                } else {
                    Some("fallback")
                }
            }
            (true, false) => {
                eprintln!("Default processor is unhealthy, using fallback");
                Some("fallback")
            }
            (false, true) => {
                eprintln!("Fallback processor is unhealthy, using default");
                Some("default")
            }
        },
        (Some(default), None) => {
            if default.failing {
                eprintln!("Default processor is unhealthy and fallback is unavailable");
                None
            } else {
                Some("default")
            }
        }
        (None, Some(fallback)) => {
            if fallback.failing {
                eprintln!("Fallback processor is unhealthy and default is unavailable");
                None
            } else {
                Some("fallback")
            }
        }
        (None, None) => {
            eprintln!("No processor health data available");
            None
        }
    }
}

async fn payment_worker(
    worker_id: usize,
    http_client: reqwest::Client,
    redis_client: RedisClient,
    health_data: Arc<Mutex<HashMap<String, HealthResponse>>>,
) {
    println!("Worker {} started", worker_id);
    loop {
        // Get a Redis connection for the entire payment processing
        let mut redis_conn = match redis_client.get_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Worker {} failed to get Redis connection: {}", worker_id, e);
                continue;
            }
        };

        // Pop payment from queue
        let payment_data: Option<PaymentRequest> = match redis_conn
            .blpop::<_, (String, Vec<u8>)>("payments_queue", 0f64)
            .await
        {
            Ok((_, payload)) => match serde_json::from_slice(&payload) {
                Ok(payment) => Some(payment),
                Err(e) => {
                    eprintln!("Worker {} failed to deserialize payment: {}", worker_id, e);
                    None
                }
            },
            Err(e) => {
                eprintln!("Worker {} failed to pop from Redis queue: {}", worker_id, e);
                None
            }
        };

        if let Some(mut payment_data) = payment_data {
            let lock_key = "payments_summary_lock";
            while is_redis_locked(&redis_client, lock_key).await {
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

                            // If processor was marked as failing, update health to healthy
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

                            // If processor was marked as failing, update health to unhealthy
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

                    // Store payment in Redis: amount in HSET, timestamp in ZADD
                    let redis_store_start = std::time::Instant::now();
                    let id = &payment_data.correlation_id;
                    let amount = payment_data.amount;
                    let timestamp_ms = payment_data
                        .requested_at
                        .map(|dt| dt.timestamp_millis() as f64)
                        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as f64);

                    let pipe_result: redis::RedisResult<()> = pipe()
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
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

// Attempts to acquire a Redis lock with a given key and TTL (in seconds)
async fn acquire_redis_lock(redis: &RedisClient, key: &str, ttl_secs: usize) -> bool {
    if let Ok(mut conn) = redis.get_async_connection().await {
        let result: Result<Option<String>, _> = redis::cmd("SET")
            .arg(key)
            .arg("locked")
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await;
        return matches!(result, Ok(Some(_)));
    }
    false
}

// Releases a Redis lock by deleting the key
async fn release_redis_lock(redis: &RedisClient, key: &str) {
    if let Ok(mut conn) = redis.get_async_connection().await {
        let _: Result<(), _> = conn.del(key).await;
    }
}

// Checks if a Redis lock is currently held
async fn is_redis_locked(redis: &RedisClient, key: &str) -> bool {
    if let Ok(mut conn) = redis.get_async_connection().await {
        match conn.get::<_, Option<String>>(key).await {
            Ok(Some(_)) => true,
            _ => false,
        }
    } else {
        false
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:password@localhost/rinha25".to_string());

    let manager = PostgresConnectionManager::new_from_stringlike(database_url, NoTls)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let db = Pool::builder()
        .build(manager)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e))?;

    let http_client = reqwest::Client::builder()
        .build()
        .expect("Failed to create HTTP client");

    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set");
    let redis_client = RedisClient::open(redis_url).expect("Failed to connect to Redis");

    let health_data = Arc::new(Mutex::new(HashMap::new()));
    for worker_id in 0..2 {
        tokio::spawn(payment_worker(
            worker_id,
            http_client.clone(),
            redis_client.clone(),
            health_data.clone(),
        ));
    }

    let app_state = AppState {
        http_client: http_client.clone(),
        redis_client: redis_client.clone(),
        db: db.clone(),
        payment_queue: mpsc::unbounded_channel::<PaymentRequest>().0, // unused, kept for compatibility
        health_data: health_data.clone(),
    };

    health_checker(health_data, http_client);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app_state.clone()))
            .service(create_payment)
            .service(get_payment_summary)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
