-- Initialize PostgreSQL test database with sample data

CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    balance DECIMAL(10, 2) DEFAULT 0.00
);

-- Insert sample data
INSERT INTO customers (name, email, created_at, is_active, balance) VALUES
('Alice Johnson', 'alice@example.com', '2024-01-15 10:30:00', TRUE, 1250.50),
('Bob Smith', 'bob@example.com', '2024-02-20 14:45:00', TRUE, 3500.00),
('Charlie Brown', 'charlie@example.com', '2024-03-10 09:15:00', FALSE, 0.00),
('Diana Prince', 'diana@example.com', '2024-04-05 16:20:00', TRUE, 750.25),
('Eve Martinez', 'eve@example.com', '2024-05-12 11:00:00', TRUE, 2100.75);

-- Create a larger table for partition testing
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending'
);

-- Insert sample orders
INSERT INTO orders (customer_id, order_date, total_amount, status)
SELECT
    (random() * 4 + 1)::INT as customer_id,
    NOW() - (random() * interval '90 days') as order_date,
    (random() * 1000 + 10)::DECIMAL(10, 2) as total_amount,
    CASE (random() * 3)::INT
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'completed'
        ELSE 'cancelled'
    END as status
FROM generate_series(1, 1000);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO test_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO test_user;
