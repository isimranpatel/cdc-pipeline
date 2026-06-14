-- ============================================================
-- setup_postgres.sql
-- Configures PostgreSQL for logical replication (WAL-based CDC)
-- ============================================================

-- Create replication user
CREATE USER replicator WITH REPLICATION LOGIN PASSWORD 'replicator_pass';

-- Create source tables
CREATE TABLE IF NOT EXISTS public.orders (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product     VARCHAR(255) NOT NULL,
    quantity    INTEGER NOT NULL,
    amount      DECIMAL(10, 2) NOT NULL,
    status      VARCHAR(50) DEFAULT 'pending',
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.customers (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    email      VARCHAR(255) UNIQUE NOT NULL,
    region     VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Grant access to replication user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
GRANT USAGE ON SCHEMA public TO replicator;

-- Create publication for CDC (all tables)
CREATE PUBLICATION cdc_publication FOR ALL TABLES;

-- Create replication slot for Debezium
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

-- Verify setup
SELECT slot_name, plugin, slot_type, active
FROM pg_replication_slots;

SELECT pubname, puballtables
FROM pg_publication;

-- Seed some initial data
INSERT INTO public.customers (name, email, region) VALUES
    ('Alice Johnson', 'alice@example.com', 'US-East'),
    ('Bob Smith',     'bob@example.com',   'US-West'),
    ('Carol White',   'carol@example.com', 'EU');

INSERT INTO public.orders (customer_id, product, quantity, amount, status) VALUES
    (1, 'Laptop',     1, 1299.99, 'completed'),
    (2, 'Headphones', 2,  199.98, 'pending'),
    (3, 'Monitor',    1,  349.99, 'shipped');
