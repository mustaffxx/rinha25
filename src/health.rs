use crate::models::{HealthResponse, PaymentRequest};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

pub fn start_health_checker(
    health_data: Arc<Mutex<HashMap<String, HealthResponse>>>,
    client: reqwest::Client,
) {
    let health_data_default = health_data.clone();
    let client_default = client.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
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

    let health_data_fallback = health_data.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
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

pub async fn check_processor_health(
    client: &reqwest::Client,
    url: &str,
) -> Result<HealthResponse, Box<dyn std::error::Error + Send + Sync>> {
    let response = client.get(url).send().await?;

    Ok(response.json().await?)
}

pub async fn get_processor_healthy(
    health_data: &Arc<Mutex<HashMap<String, HealthResponse>>>,
    processor: &str,
) -> Option<HealthResponse> {
    let map = health_data.lock().await;
    map.get(processor).cloned()
}

pub async fn get_process_payment(
    _http_client: &reqwest::Client,
    health_data: &Arc<Mutex<HashMap<String, HealthResponse>>>,
    _payment_data: &PaymentRequest,
) -> Option<&'static str> {
    let default = get_processor_healthy(health_data, "default").await;
    let fallback = get_processor_healthy(health_data, "fallback").await;

    match (
        default.as_ref().filter(|d| !d.failing),
        fallback.as_ref().filter(|f| !f.failing),
    ) {
        (Some(d), Some(f)) => {
            if d.min_response_time <= f.min_response_time {
                Some("default")
            } else {
                Some("fallback")
            }
        }
        (Some(_), None) => Some("default"),
        (None, Some(_)) => Some("fallback"),
        _ => None,
    }
}
