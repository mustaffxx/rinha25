use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::Mutex;

use crate::health_worker::{HealthWorker, PaymentProcessor};

#[derive(Serialize, Deserialize)]
pub struct PaymentRequest {
    #[serde(rename = "correlationId")]
    correlation_id: String,
    amount: f64,
}

#[derive(Clone)]
pub struct PaymentService {
    health_worker: Arc<HealthWorker>,
    http_client: reqwest::Client,
    payments_fifo: Arc<Mutex<VecDeque<PaymentRequest>>>,
}

impl PaymentService {
    pub fn new(
        health_worker: Arc<HealthWorker>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("Failed to create HTTP client");
        let payments_fifo = Arc::new(Mutex::new(VecDeque::new()));

        Ok(PaymentService {
            health_worker,
            http_client,
            payments_fifo,
        })
    }

    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.run().await;
        })
    }

    pub async fn pub_payment_request(&self, payment_request: PaymentRequest) -> Result<(), String> {
        let mut fifo = self.payments_fifo.lock().await;
        fifo.push_back(payment_request);
        Ok(())
    }

    async fn run(self) {
        loop {
            let maybe_request = {
                let mut fifo = self.payments_fifo.lock().await;
                fifo.pop_front()
            };

            if let Some(payment_request) = maybe_request {
                let payment_data: serde_json::Value = serde_json::to_value(payment_request)
                    .expect("Failed to serialize payment request");

                if let Err(e) = self.process_payments(payment_data).await {
                    if !e.contains("No available payment processor") {
                        eprintln!("Error processing payment: {}", e);
                    }
                }
            }
        }
    }

    async fn process_payments(&self, payment_data: serde_json::Value) -> Result<(), String> {
        let payment_processor = self.health_worker.get_payment_processor();

        match payment_processor {
            PaymentProcessor::Default => {
                self.send_payment(
                    "http://payment-processor-default:8080/payments",
                    payment_data,
                )
                .await
            }
            PaymentProcessor::Fallback => {
                self.send_payment(
                    "http://payment-processor-fallback:8080/payments",
                    payment_data,
                )
                .await
            }
            PaymentProcessor::None => Err("No available payment processor".to_string()),
        }
    }

    async fn send_payment(&self, url: &str, data: serde_json::Value) -> Result<(), String> {
        let response = self
            .http_client
            .post(url)
            .json(&data)
            .send()
            .await
            .map_err(|e| format!("Error sending payment request: {}", e))?;

        if response.status().is_success() {
            response
                .json()
                .await
                .map_err(|e| format!("Failed to parse response: {}", e))?
        } else {
            Err(format!("Failed to send payment: {}", response.status()))
        }
    }
}
