# rinha25

A Rust-based payment processing service using PostgreSQL.

## Overview

This service handles payment requests asynchronously using worker threads and provides a summary endpoint for payment statistics. The service has been migrated from SQLite/LibSQL to PostgreSQL for better performance and scalability.

## Architecture

- **Web Framework**: Actix-web
- **Database**: PostgreSQL with connection pooling (bb8)
- **Cache**: Memcached for processor health status
- **HTTP Client**: Reqwest for communication with payment processors
- **Async Processing**: Tokio-based workers for payment processing

## Database Schema

The service uses a PostgreSQL database with the following table:

```sql
CREATE TABLE payment_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processor VARCHAR(20) NOT NULL CHECK (processor IN ('default', 'fallback')),
    amount NUMERIC(10, 2) NOT NULL,
    correlation_id TEXT NOT NULL
);
```

## Environment Variables

- `DATABASE_URL`: PostgreSQL connection string (default: `postgres://postgres:password@localhost/rinha25`)
- `CACHE_URL`: Memcached connection URL (required)

## Running with Docker

```bash
docker-compose up -d
```

The service will be available at `http://localhost:9999`

## API Endpoints

- `POST /payments` - Submit a payment request
- `GET /payments-summary` - Get payment statistics with optional date filtering

## Changes from SQLite to PostgreSQL

1. **Database Dependencies**: Replaced `libsql` with `tokio-postgres`, `bb8`, and `bb8-postgres`
2. **Connection Management**: Uses connection pooling instead of direct database connections
3. **Data Types**: Updated to use PostgreSQL-specific types (UUID, NUMERIC, TIMESTAMP WITH TIME ZONE)
4. **SQL Syntax**: Updated queries to use PostgreSQL parameter syntax (`$1`, `$2`, etc.)
5. **Docker Configuration**: Replaced SQLite container with PostgreSQL container