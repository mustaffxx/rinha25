use actix_web::{App, HttpServer, web};
use rinha25::api::{create_payment, get_payment_summary};
use rinha25::models::{AppState, PaymentRequest};
use rinha25::worker::payment_worker;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set");
    let redis_pool = rinha25::redis_helpers::create_redis_pool(&redis_url)?;
    rinha25::redis_helpers::clean_redis(&redis_pool)
        .await
        .expect("Failed to clean Redis");

    let http_client = reqwest::Client::builder()
        .build()
        .expect("Failed to create HTTP client");

    let (payment_sender, payment_receiver) = mpsc::channel::<PaymentRequest>(100_000);
    let payment_receiver = Arc::new(Mutex::new(payment_receiver));

    for worker_id in 0..10 {
        tokio::spawn(payment_worker(
            worker_id,
            http_client.clone(),
            redis_pool.clone(),
            payment_receiver.clone(),
        ));
    }

    let app_state = AppState::new(
        http_client.clone(),
        redis_pool.clone(),
        payment_sender.clone(),
    );

    println!("API started and listening on 0.0.0.0:8080");

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
