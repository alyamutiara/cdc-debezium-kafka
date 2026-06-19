-- E-commerce schema for CDC pipeline demo
-- Relatable domain demonstrating multi-table CDC with FK relationships

CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    ssn VARCHAR(20),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(64) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    price_cents INTEGER NOT NULL CHECK (price_cents >= 0),
    internal_notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    total_cents INTEGER NOT NULL,
    internal_notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price_cents INTEGER NOT NULL
);

-- Seed data
INSERT INTO customers (email, full_name, password_hash, ssn) VALUES
    ('alice@example.com', 'Alice Smith', '$2b$12$dummyhash1', '111-22-3333'),
    ('bob@example.com', 'Bob Jones', '$2b$12$dummyhash2', '222-33-4444'),
    ('carol@example.com', 'Carol Lee', '$2b$12$dummyhash3', '333-44-5555')
ON CONFLICT DO NOTHING;

INSERT INTO products (sku, name, price_cents, internal_notes) VALUES
    ('SKU-001', 'Mechanical Keyboard', 12999, 'high margin'),
    ('SKU-002', 'Wireless Mouse', 4999, 'best seller'),
    ('SKU-003', 'USB-C Hub', 3499, 'internal: do not discount')
ON CONFLICT DO NOTHING;

INSERT INTO orders (customer_id, status, total_cents, internal_notes) VALUES
    (1, 'pending', 12999, 'awaiting payment'),
    (2, 'shipped', 4999, NULL),
    (3, 'delivered', 3499, 'gift wrapped')
ON CONFLICT DO NOTHING;

INSERT INTO order_items (order_id, product_id, quantity, price_cents) VALUES
    (1, 1, 1, 12999),
    (2, 2, 1, 4999),
    (3, 3, 1, 3499)
ON CONFLICT DO NOTHING;
