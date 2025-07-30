use actix_web::{App, HttpServer, web};
use rinha25::api::{create_payment, get_payment_summary};
use rinha25::health::start_health_checker;
use rinha25::models::{AppState, PaymentRequest};
use rinha25::worker::payment_worker;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL environment variable must be set");

    let manager = rinha25::models::create_pg_manager(&database_url)?;
    let db = rinha25::models::create_db_pool(manager).await?;

    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set");
    let redis_pool = rinha25::models::create_redis_pool(&redis_url)?;

    let health_data = Arc::new(Mutex::new(HashMap::new()));

    let http_client = reqwest::Client::builder()
        .build()
        .expect("Failed to create HTTP client");

    start_health_checker(health_data.clone(), http_client.clone());

    for worker_id in 0..5 {
        tokio::spawn(payment_worker(
            worker_id,
            http_client.clone(),
            redis_pool.clone(),
            health_data.clone(),
        ));
    }

    let app_state = AppState::new(
        http_client.clone(),
        redis_pool.clone(),
        db.clone(),
        mpsc::unbounded_channel::<PaymentRequest>().0,
        health_data.clone(),
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
