DROP TABLE IF EXISTS payment_events;

CREATE TABLE payment_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processor VARCHAR(20) NOT NULL CHECK (processor IN ('default', 'fallback')),
    amount NUMERIC(10, 2) NOT NULL,
    correlation_id TEXT NOT NULL
);

CREATE INDEX idx_payment_events_timestamp_processor ON payment_events (timestamp, processor);