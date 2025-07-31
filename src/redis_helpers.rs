use deadpool_redis::Config as RedisConfig;
use deadpool_redis::Pool as RedisPool;
use deadpool_redis::redis::AsyncCommands;

pub async fn acquire_redis_lock(redis_pool: &RedisPool, key: &str, ttl_secs: usize) -> bool {
    if let Ok(mut conn) = redis_pool.get().await {
        let result: Result<Option<String>, _> = deadpool_redis::redis::cmd("SET")
            .arg(key)
            .arg("locked")
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs)
            .query_async(&mut *conn)
            .await;
        return matches!(result, Ok(Some(_)));
    }
    false
}

pub async fn release_redis_lock(redis_pool: &RedisPool, key: &str) {
    if let Ok(mut conn) = redis_pool.get().await {
        let _: Result<(), _> = conn.del(key).await;
    }
}

pub async fn is_redis_locked(redis_pool: &RedisPool, key: &str) -> bool {
    if let Ok(mut conn) = redis_pool.get().await {
        match conn.get::<_, Option<String>>(key).await {
            Ok(Some(_)) => true,
            Ok(None) => false,
            _ => false,
        }
    } else {
        false
    }
}

pub fn create_redis_pool(redis_url: &str) -> Result<RedisPool, std::io::Error> {
    let cfg = RedisConfig::from_url(redis_url);

    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .map_err(|e| {
            eprintln!("Failed to create Redis pool ({}): {}", redis_url, e);
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
        })
}

pub async fn clean_redis(redis_pool: &RedisPool) -> Result<(), std::io::Error> {
    let mut conn: deadpool_redis::Connection = redis_pool.get().await.map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "Failed to get Redis connection",
        )
    })?;

    conn.flushall()
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Failed to clean Redis"))
}
