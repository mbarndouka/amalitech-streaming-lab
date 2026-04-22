-- 1. Create the database (Run this first, then connect to it)
-- CREATE DATABASE ecommerce;

-- 2. Connect to the database (if using psql CLI: \c ecommerce)

-- 3. Create the table
CREATE TABLE IF NOT EXISTS ecommerce_events (
    event_id UUID PRIMARY KEY,
    user_id INT NOT NULL,
    action VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    -- We add this column to track exactly when Spark processed the row
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM ecommerce_events WHERE =user_id = 999;