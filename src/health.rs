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

pub async fn check_processor_health(
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
