use actix_web::{App, HttpResponse, HttpServer, Responder, get, web};
use serde::{Deserialize, Serialize};

mod health_worker;

#[derive(Serialize, Deserialize)]
struct PaymentRequest {
    #[serde(rename = "correlationId")]
    correlation_id: String,
    amount: f64,
}

#[get("/payments")]
async fn get_payments(payment: web::Json<PaymentRequest>) -> impl Responder {
    HttpResponse::Ok().json(payment.into_inner())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let health_worker = health_worker::HealthWorker::new().expect("Failed to create HealthWorker");
    health_worker.start();

    HttpServer::new(|| App::new().service(get_payments))
        .bind(("127.0.0.1", 9999))?
        .run()
        .await
}
