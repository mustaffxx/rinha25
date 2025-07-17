use actix_web::{App, HttpResponse, HttpServer, Result, web};
use chrono::{DateTime, Utc};
use libsql::{Builder, Database};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;

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
}

#[derive(Serialize, Deserialize)]
struct HealthResponse {
    failing: bool,
    #[serde(rename = "minResponseTime")]
    min_response_time: u64,
}

#[derive(Clone)]
struct AppState {
    http_client: reqwest::Client,
    cache_client: memcache::Client,
    db: Arc<Database>,
    payment_queue: mpsc::UnboundedSender<PaymentRequest>,
}

#[actix_web::post("/payments")]
async fn create_payment(
    payment: web::Json<PaymentRequest>,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    let payment_data = payment.into_inner();
    let state = data.get_ref();

    if let Err(e) = state.payment_queue.send(payment_data) {
        eprintln!("Failed to queue payment: {}", e);
        return Ok(
            HttpResponse::InternalServerError().json(serde_json::json!({"status": "queue_error"}))
        );
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({"status": "accepted"})))
}

#[actix_web::get("/payments-summary")]
async fn get_payment_summary(
    query: web::Query<SummaryQuery>,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    let summary = fetch_payment_summary(&data.db, query.from, query.to)
        .await
        .map_err(|e| {
            eprintln!("Database error in payments-summary: {}", e);
            actix_web::error::ErrorInternalServerError(format!("Database error: {}", e))
        })?;

    Ok(HttpResponse::Ok().json(summary))
}

async fn record_payment(
    db: &Database,
    processor: &str,
    payment: &PaymentRequest,
) -> Result<(), libsql::Error> {
    let conn = db.connect()?;
    match conn
        .execute(
            "INSERT INTO payment_events (processor, amount, correlation_id) VALUES (?, ?, ?)",
            [
                processor,
                &payment.amount.to_string(),
                &payment.correlation_id,
            ],
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Failed to insert payment event: {}", e);
            Err(e)
        }
    }
}

async fn record_payment_with_conn(
    conn: &libsql::Connection,
    processor: &str,
    payment: &PaymentRequest,
) -> Result<(), libsql::Error> {
    match conn
        .execute(
            "INSERT INTO payment_events (processor, amount, correlation_id) VALUES (?, ?, ?)",
            [
                processor,
                &payment.amount.to_string(),
                &payment.correlation_id,
            ],
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Failed to insert payment event: {}", e);
            Err(e)
        }
    }
}

async fn fetch_payment_summary(
    db: &Database,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> Result<PaymentSummary, libsql::Error> {
    let conn = db.connect()?;

    let query = "
        SELECT 
            processor, 
            COUNT(*) AS total_requests, 
            COALESCE(SUM(amount), 0) AS total_amount 
        FROM payment_events 
        WHERE (? IS NULL OR timestamp >= ?)
          AND (? IS NULL OR timestamp <= ?)
        GROUP BY processor";

    let from_str = from.map(|dt| dt.to_rfc3339());
    let to_str = to.map(|dt| dt.to_rfc3339());

    let mut stmt: libsql::Statement = conn.prepare(query).await?;
    let mut rows = stmt
        .query([
            from_str.as_deref(),
            from_str.as_deref(),
            to_str.as_deref(),
            to_str.as_deref(),
        ])
        .await?;

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

    while let Some(row) = rows.next().await? {
        let processor: String = row.get(0)?;
        let requests = row.get::<i64>(1)? as u64;
        let amount = row.get::<f64>(2)?;

        match processor.as_str() {
            "default" => {
                summary.default.total_requests = requests;
                summary.default.total_amount = amount;
            }
            "fallback" => {
                summary.fallback.total_requests = requests;
                summary.fallback.total_amount = amount;
            }
            _ => {}
        }
    }

    Ok(summary)
}

fn is_processor_healthy(
    cache: &mut memcache::Client,
    processor: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let key = format!("health_{}", processor);

    if let Ok(Some(data)) = cache.get::<Vec<u8>>(&key) {
        if let Ok(health) = serde_json::from_slice::<HealthResponse>(&data) {
            return Ok(!health.failing);
        }
    }

    Ok(false)
}

async fn send_payment(
    client: &reqwest::Client,
    url: &str,
    payment: &PaymentRequest,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let response = client
        .post(url)
        .json(payment)
        .timeout(Duration::from_millis(200))
        .send()
        .await?;

    if response.status().is_success() {
        println!("Payment processed successfully: {}", payment.correlation_id);
        Ok(())
    } else {
        Err(format!("Payment failed with status: {}", response.status()).into())
    }
}

async fn health_checker(cache: memcache::Client, client: reqwest::Client) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        let (default_result, fallback_result) = tokio::join!(
            check_processor_health(
                &client,
                "http://payment-processor-default:8080/payments/service-health"
            ),
            check_processor_health(
                &client,
                "http://payment-processor-fallback:8080/payments/service-health"
            )
        );

        if let Ok(health) = default_result {
            let vec_health = serde_json::to_vec(&health).unwrap();
            if let Err(e) = cache.set("health_default", &vec_health[..], 10) {
                eprintln!("Failed to set cache for health_default: {}", e);
            }
        }

        if let Ok(health) = fallback_result {
            let vec_health = serde_json::to_vec(&health).unwrap();
            if let Err(e) = cache.set("health_fallback", &vec_health[..], 10) {
                eprintln!("Failed to set cache for health_fallback: {}", e);
            }
        }
    }
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

async fn payment_worker(
    worker_id: usize,
    shared_receiver: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<PaymentRequest>>>,
    http_client: reqwest::Client,
    cache_client: memcache::Client,
    database: Arc<Database>,
) {
    let db_conn = match database.connect() {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("Worker {} failed to connect to database: {}", worker_id, e);
            return;
        }
    };

    println!(
        "Worker {} started with its own database connection",
        worker_id
    );

    loop {
        let payment = {
            let mut rx = shared_receiver.lock().await;
            rx.recv().await
        };

        match payment {
            Some(payment_data) => {
                println!(
                    "Worker {} processing payment: {}",
                    worker_id, payment_data.correlation_id
                );

                let mut successful_processor = None;
                let mut retry_count = 0u32;
                let base_delay_ms = 100u64;
                let max_delay_ms = 5000u64;

                loop {
                    let mut cache_clone = cache_client.clone();
                    if is_processor_healthy(&mut cache_clone, "default").unwrap_or(false)
                        && send_payment(
                            &http_client,
                            "http://payment-processor-default:8080/payments",
                            &payment_data,
                        )
                        .await
                        .is_ok()
                    {
                        successful_processor = Some("default");
                    } else {
                        let mut cache_clone = cache_client.clone();
                        if is_processor_healthy(&mut cache_clone, "fallback").unwrap_or(false)
                            && send_payment(
                                &http_client,
                                "http://payment-processor-fallback:8080/payments",
                                &payment_data,
                            )
                            .await
                            .is_ok()
                        {
                            successful_processor = Some("fallback");
                        }
                    }

                    if let Some(processor) = successful_processor {
                        if let Err(e) =
                            record_payment_with_conn(&db_conn, processor, &payment_data).await
                        {
                            eprintln!("Worker {} failed to record payment: {}", worker_id, e);
                        }
                        break;
                    }

                    retry_count += 1;
                    let delay_ms = std::cmp::min(
                        base_delay_ms * 2u64.pow(std::cmp::min(retry_count, 10)),
                        max_delay_ms,
                    );

                    let jitter = (delay_ms as f64 * 0.1 * rand::random::<f64>()) as u64;
                    let final_delay = delay_ms + jitter;

                    if retry_count % 10 == 0 {
                        eprintln!(
                            "Worker {} retrying payment {} (attempt {}), waiting {}ms",
                            worker_id, payment_data.correlation_id, retry_count, final_delay
                        );
                    }

                    tokio::time::sleep(Duration::from_millis(final_delay)).await;
                }
            }
            None => {
                println!("Worker {} shutting down", worker_id);
                break;
            }
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let database_path =
        std::env::var("DATABASE_PATH").unwrap_or_else(|_| "/data/local.db".to_string());

    let db = Builder::new_local(database_path)
        .build()
        .await
        .expect("Failed to connect to database");

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("Failed to create HTTP client");

    let cache_url = std::env::var("CACHE_URL").expect("CACHE_URL must be set");
    let cache_client =
        memcache::Client::connect(cache_url).expect("Failed to connect to memcached");

    let (payment_sender, payment_receiver) = mpsc::unbounded_channel::<PaymentRequest>();
    let shared_receiver = Arc::new(tokio::sync::Mutex::new(payment_receiver));
    let shared_db = Arc::new(db);

    for worker_id in 0..10 {
        tokio::spawn(payment_worker(
            worker_id,
            shared_receiver.clone(),
            http_client.clone(),
            cache_client.clone(),
            shared_db.clone(),
        ));
    }

    let app_state = AppState {
        http_client: http_client.clone(),
        cache_client: cache_client.clone(),
        db: shared_db.clone(),
        payment_queue: payment_sender,
    };

    tokio::spawn(health_checker(cache_client, http_client));

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
