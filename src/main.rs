use actix_web::{App, HttpResponse, HttpServer, Result, web};
use serde::{Deserialize, Serialize};
use std::time::Duration;

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
}

#[actix_web::post("/payments")]
async fn create_payment(
    payment: web::Json<PaymentRequest>,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    let payment_data = payment.into_inner();
    let state = data.get_ref();

    tokio::spawn(process_payment_async(
        state.http_client.clone(),
        state.cache_client.clone(),
        payment_data,
    ));

    Ok(HttpResponse::Ok().json(serde_json::json!({"status": "accepted"})))
}

async fn process_payment_async(
    client: reqwest::Client,
    mut cache: memcache::Client,
    payment: PaymentRequest,
) {
    loop {
        if is_processor_healthy(&mut cache, "default").unwrap_or(false)
            && send_payment(
                &client,
                "http://payment-processor-default:8080/payments",
                &payment,
            )
            .await
            .is_ok()
        {
            return;
        }

        if is_processor_healthy(&mut cache, "fallback").unwrap_or(false)
            && send_payment(
                &client,
                "http://payment-processor-fallback:8080/payments",
                &payment,
            )
            .await
            .is_ok()
        {
            return;
        }
    }
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
        .timeout(Duration::from_millis(100))
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
            let _ = cache.set("health_default", &vec_health[..], 10);
        }

        if let Ok(health) = fallback_result {
            let vec_health = serde_json::to_vec(&health).unwrap();
            let _ = cache.set("health_fallback", &vec_health[..], 10);
        }
    }
}

async fn check_processor_health(
    client: &reqwest::Client,
    url: &str,
) -> Result<HealthResponse, Box<dyn std::error::Error + Send + Sync>> {
    let response = client
        .get(url)
        .timeout(Duration::from_millis(100))
        .send()
        .await?;

    Ok(response.json().await?)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("Failed to create HTTP client");

    let cache_client =
        memcache::Client::connect("127.0.0.1:11211").expect("Failed to connect to memcached");

    let app_state = AppState {
        http_client: http_client.clone(),
        cache_client: cache_client.clone(),
    };

    tokio::spawn(health_checker(cache_client, http_client));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app_state.clone()))
            .service(create_payment)
    })
    .bind(("127.0.0.1", 9999))?
    .run()
    .await
}
