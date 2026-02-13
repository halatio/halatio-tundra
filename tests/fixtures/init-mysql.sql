-- Initialize MySQL test database with sample data

CREATE TABLE IF NOT EXISTS customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
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
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

-- Insert sample orders (MySQL doesn't have generate_series, so we use a different approach)
INSERT INTO orders (customer_id, order_date, total_amount, status)
SELECT
    FLOOR(1 + (RAND() * 5)) as customer_id,
    DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 90) DAY) as order_date,
    ROUND(10 + (RAND() * 1000), 2) as total_amount,
    ELT(FLOOR(1 + (RAND() * 3)), 'pending', 'completed', 'cancelled') as status
FROM
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t3,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t4,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t5,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) t6
LIMIT 1000;
