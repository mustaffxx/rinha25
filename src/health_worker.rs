use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PaymentProcessor {
    Default,
    Fallback,
    None,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct HealthServiceResponse {
    failing: bool,
    #[serde(rename = "minResponseTime")]
    min_response_time: u64,
}

#[derive(Clone)]
pub struct HealthWorker {
    client: Arc<Mutex<memcache::Client>>,
    http_client: reqwest::Client,
}

impl HealthWorker {
    pub fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = memcache::Client::connect("127.0.0.1:11211")?;
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;

        Ok(HealthWorker {
            client: Arc::new(Mutex::new(client)),
            http_client,
        })
    }

    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.run().await;
        })
    }

    async fn run(&self) {
        let mut interval = time::interval(Duration::from_secs(5));

        loop {
            let (default_result, fallback_result) = tokio::join!(
                self.update_service_health(
                    "default",
                    "http://payment-processor-default:8080/payments/service-health"
                ),
                self.update_service_health(
                    "fallback",
                    "http://payment-processor-fallback:8080/payments/service-health"
                )
            );

            if let Err(e) = default_result {
                eprintln!("Failed to update default health: {}", e);
            }
            if let Err(e) = fallback_result {
                eprintln!("Failed to update fallback health: {}", e);
            }

            interval.tick().await;
        }
    }

    async fn update_service_health(
        &self,
        cache_key: &str,
        url: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self.fetch_health(url).await {
            Ok(health) => {
                let data = serde_json::to_vec(&health)?;
                let client = self.client.lock().map_err(|_| "Mutex poisoned")?;
                client.set(cache_key, data.as_slice(), 5)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn get_payment_processor(&self) -> PaymentProcessor {
        if let Ok(health) = self.get_cached_health("default") {
            if !health.failing {
                return PaymentProcessor::Default;
            }
        }

        if let Ok(health) = self.get_cached_health("fallback") {
            if !health.failing {
                return PaymentProcessor::Fallback;
            }
        }

        PaymentProcessor::None
    }

    fn get_cached_health(
        &self,
        key: &str,
    ) -> Result<HealthServiceResponse, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.lock().map_err(|_| "Mutex poisoned")?;
        let data: Vec<u8> = client.get(key)?.ok_or("Cache miss")?;
        let health = serde_json::from_slice(&data)?;
        Ok(health)
    }

    async fn fetch_health(
        &self,
        url: &str,
    ) -> Result<HealthServiceResponse, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.http_client.get(url).send().await?;

        if !response.status().is_success() {
            return Err(format!("HTTP error: {}", response.status()).into());
        }

        let health = response.json::<HealthServiceResponse>().await?;
        Ok(health)
    }
}
