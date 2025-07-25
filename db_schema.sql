DROP TABLE IF EXISTS payment_events;

CREATE TABLE payment_events (
    correlation_id TEXT PRIMARY KEY,
    requested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processor VARCHAR(20) NOT NULL CHECK (processor IN ('default', 'fallback')),
    amount NUMERIC(10, 2) NOT NULL
);

CREATE INDEX idx_payment_events_requested_at_processor ON payment_events (requested_at, processor);