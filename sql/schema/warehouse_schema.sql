-- Data Warehouse Schema (Star Schema)
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Date Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Customer Dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    customer_segment VARCHAR(50),
    is_active BOOLEAN,
    registration_date DATE,
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON warehouse.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON warehouse.dim_customer(is_current);

-- Product Dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_product_id ON warehouse.dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_current ON warehouse.dim_product(is_current);

-- Payment Method Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method VARCHAR(50) UNIQUE NOT NULL,
    payment_type VARCHAR(50),
    processing_fee_pct DECIMAL(5,2)
);

-- Shipping Method Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_shipping_method (
    shipping_method_key SERIAL PRIMARY KEY,
    shipping_method VARCHAR(50) UNIQUE NOT NULL,
    estimated_days INTEGER,
    base_cost DECIMAL(10,2)
);

-- Order Fact Table
CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    order_date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    customer_key INTEGER REFERENCES warehouse.dim_customer(customer_key),
    payment_method_key INTEGER REFERENCES warehouse.dim_payment_method(payment_method_key),
    shipping_method_key INTEGER REFERENCES warehouse.dim_shipping_method(shipping_method_key),
    order_quantity INTEGER,
    subtotal_amount DECIMAL(12,2),
    shipping_cost DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    order_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON warehouse.fact_orders(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON warehouse.fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_id ON warehouse.fact_orders(order_id);

-- Order Line Items Fact Table
CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    order_item_key SERIAL PRIMARY KEY,
    order_key INTEGER REFERENCES warehouse.fact_orders(order_key),
    product_key INTEGER REFERENCES warehouse.dim_product(product_key),
    order_date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    unit_cost DECIMAL(10,2),
    line_total DECIMAL(12,2),
    discount_amount DECIMAL(10,2),
    profit DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_order ON warehouse.fact_order_items(order_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_product ON warehouse.fact_order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_date ON warehouse.fact_order_items(order_date_key);