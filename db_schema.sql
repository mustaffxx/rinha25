DROP TABLE IF EXISTS payment_events;

CREATE TABLE payment_events (
    id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processor TEXT NOT NULL CHECK (processor IN ('default', 'fallback')),
    amount REAL NOT NULL,
    correlation_id TEXT NOT NULL
);

CREATE INDEX idx_payment_events_timestamp_processor ON payment_events (timestamp, processor);