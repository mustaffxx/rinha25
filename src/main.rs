use std::sync::Arc;

use actix_web::{App, HttpResponse, HttpServer, Responder, post, web};

use crate::payment_service::PaymentRequest;

mod health_worker;
mod payment_service;

#[post("/payments")]
async fn create_payment(
    payment: web::Json<PaymentRequest>,
    payment_service: web::Data<Arc<payment_service::PaymentService>>,
) -> impl Responder {
    match payment_service
        .pub_payment_request(payment.into_inner())
        .await
    {
        Ok(()) => HttpResponse::Ok().finish(),
        Err(e) => HttpResponse::InternalServerError().body(e),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let health_worker =
        Arc::new(health_worker::HealthWorker::new().expect("Failed to create HealthWorker"));

    health_worker.start();

    let payment_service = payment_service::PaymentService::new(health_worker.clone())
        .expect("Failed to create PaymentService");

    payment_service.start();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(payment_service.clone()))
            .service(create_payment)
    })
    .bind(("127.0.0.1", 9999))?
    .run()
    .await
}
