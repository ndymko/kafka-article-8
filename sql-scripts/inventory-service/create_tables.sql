CREATE TABLE inventory (
    id BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(256) NOT NULL UNIQUE,
    quantity INT NOT NULL CHECK (quantity >= 0)
);

CREATE TABLE processed_order_id (
    order_id VARCHAR(36) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE outbox_event (
    key VARCHAR(36) PRIMARY KEY,
    topic VARCHAR(100) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    sent BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_outbox_event_sent ON outbox_event(sent, created_at);
