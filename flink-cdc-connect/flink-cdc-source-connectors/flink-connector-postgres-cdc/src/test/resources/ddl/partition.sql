/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- ================================================================================================================
-- DATABASE:  partition
-- DESCRIPTION: Test database for PostgreSQL partition table functionality and CDC integration
-- 
-- This schema demonstrates PostgreSQL partition table features including:
-- - Range partitioning by date
-- - List partitioning by category
-- - Primary key constraints on child tables (for PG10 compatibility)
-- - REPLICA IDENTITY FULL for CDC support
-- - Version-specific features (hash partitioning for PG11+)
-- ================================================================================================================

-- Create and use the partition schema
DROP SCHEMA IF EXISTS partition CASCADE;
CREATE SCHEMA partition;
SET search_path TO partition;

-- =======================================================================================
-- RANGE PARTITIONED TABLE: orders (partitioned by order_date)
-- Note: In PostgreSQL 10, primary keys cannot be defined directly on partitioned tables
-- =======================================================================================

-- Create parent partitioned table (WITHOUT primary key due to PG10 limitation)
CREATE TABLE orders (
    order_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    total_amount DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (order_date);

-- Create quarterly partitions for 2023
CREATE TABLE orders_2023_q1 PARTITION OF orders
    FOR VALUES FROM ('2023-01-01') TO ('2023-04-01');

CREATE TABLE orders_2023_q2 PARTITION OF orders
    FOR VALUES FROM ('2023-04-01') TO ('2023-07-01');

CREATE TABLE orders_2023_q3 PARTITION OF orders
    FOR VALUES FROM ('2023-07-01') TO ('2023-10-01');

CREATE TABLE orders_2023_q4 PARTITION OF orders
    FOR VALUES FROM ('2023-10-01') TO ('2024-01-01');

-- Add primary keys to child tables (PostgreSQL 10 requirement)
-- The primary key must include the partition key (order_date)
ALTER TABLE orders_2023_q1 ADD PRIMARY KEY (order_id, order_date);
ALTER TABLE orders_2023_q2 ADD PRIMARY KEY (order_id, order_date);
ALTER TABLE orders_2023_q3 ADD PRIMARY KEY (order_id, order_date);
ALTER TABLE orders_2023_q4 ADD PRIMARY KEY (order_id, order_date);

-- Set REPLICA IDENTITY FULL for CDC support on all partitions
ALTER TABLE orders_2023_q1 REPLICA IDENTITY FULL;
ALTER TABLE orders_2023_q2 REPLICA IDENTITY FULL;
ALTER TABLE orders_2023_q3 REPLICA IDENTITY FULL;
ALTER TABLE orders_2023_q4 REPLICA IDENTITY FULL;

-- Insert test data across partitions
INSERT INTO orders (order_id, customer_id, order_date, total_amount, status) VALUES
    -- Q1 2023 data
    (1001, 1, '2023-01-15', 149.99, 'completed'),
    (1002, 2, '2023-02-20', 299.50, 'shipped'),
    (1003, 3, '2023-03-10', 75.00, 'pending'),
    
    -- Q2 2023 data
    (1004, 4, '2023-04-05', 199.99, 'completed'),
    (1005, 5, '2023-05-15', 89.95, 'shipped'),
    (1006, 1, '2023-06-25', 450.00, 'pending'),
    
    -- Q3 2023 data
    (1007, 2, '2023-07-08', 325.75, 'completed'),
    (1008, 6, '2023-08-18', 159.99, 'shipped'),
    (1009, 3, '2023-09-22', 75.50, 'cancelled'),
    
    -- Q4 2023 data
    (1010, 4, '2023-10-12', 299.99, 'completed'),
    (1011, 7, '2023-11-20', 189.95, 'shipped'),
    (1012, 5, '2023-12-15', 550.00, 'pending');

-- =======================================================================================
-- LIST PARTITIONED TABLE: products_by_category (partitioned by category)
-- =======================================================================================

-- Create parent partitioned table (WITHOUT primary key due to PG10 limitation)
CREATE TABLE products_by_category (
    product_id INTEGER NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10,2),
    in_stock BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY LIST (category);

-- Create category-based partitions
CREATE TABLE products_electronics PARTITION OF products_by_category
    FOR VALUES IN ('electronics', 'computers', 'phones');

CREATE TABLE products_clothing PARTITION OF products_by_category
    FOR VALUES IN ('clothing', 'shoes', 'accessories');

CREATE TABLE products_home PARTITION OF products_by_category
    FOR VALUES IN ('home', 'garden', 'furniture');

-- Add primary keys to child tables (must include partition key)
ALTER TABLE products_electronics ADD PRIMARY KEY (product_id, category);
ALTER TABLE products_clothing ADD PRIMARY KEY (product_id, category);
ALTER TABLE products_home ADD PRIMARY KEY (product_id, category);

-- Set REPLICA IDENTITY FULL for CDC support
ALTER TABLE products_electronics REPLICA IDENTITY FULL;
ALTER TABLE products_clothing REPLICA IDENTITY FULL;
ALTER TABLE products_home REPLICA IDENTITY FULL;

-- Insert test data across partitions
INSERT INTO products_by_category (product_id, product_name, category, price, in_stock) VALUES
    -- Electronics
    (2001, 'Laptop Computer', 'electronics', 899.99, true),
    (2002, 'Smartphone', 'phones', 599.95, true),
    (2003, 'Tablet', 'computers', 299.99, false),
    (2004, 'Wireless Headphones', 'electronics', 149.99, true),
    
    -- Clothing
    (2005, 'T-Shirt', 'clothing', 19.99, true),
    (2006, 'Running Shoes', 'shoes', 89.95, true),
    (2007, 'Baseball Cap', 'accessories', 24.99, true),
    (2008, 'Jeans', 'clothing', 59.95, false),
    
    -- Home & Garden
    (2009, 'Office Chair', 'furniture', 199.99, true),
    (2010, 'Garden Hose', 'garden', 39.95, true),
    (2011, 'Coffee Table', 'furniture', 149.99, true),
    (2012, 'Plant Pot', 'garden', 12.95, true);

-- =======================================================================================
-- REGULAR NON-PARTITIONED TABLE: customers (for comparison)
-- =======================================================================================

CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    country VARCHAR(50) DEFAULT 'USA',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Set REPLICA IDENTITY FULL for CDC support
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Insert customer test data
INSERT INTO customers (customer_id, first_name, last_name, email, phone, address, city, country) VALUES
    (1, 'John', 'Doe', 'john.doe@email.com', '555-0101', '123 Main St', 'New York', 'USA'),
    (2, 'Jane', 'Smith', 'jane.smith@email.com', '555-0102', '456 Oak Ave', 'Los Angeles', 'USA'),
    (3, 'Mike', 'Johnson', 'mike.johnson@email.com', '555-0103', '789 Pine Rd', 'Chicago', 'USA'),
    (4, 'Sarah', 'Brown', 'sarah.brown@email.com', '555-0104', '321 Elm St', 'Houston', 'USA'),
    (5, 'David', 'Wilson', 'david.wilson@email.com', '555-0105', '654 Maple Dr', 'Phoenix', 'USA'),
    (6, 'Lisa', 'Davis', 'lisa.davis@email.com', '555-0106', '987 Cedar Ln', 'Philadelphia', 'USA'),
    (7, 'Robert', 'Miller', 'robert.miller@email.com', '555-0107', '147 Birch Way', 'San Antonio', 'USA');

-- =======================================================================================
-- UTILITY VIEW: All partition information
-- =======================================================================================

CREATE VIEW partition_info AS
SELECT 
    schemaname,
    tablename,
    partitioned_table,
    partition_strategy,
    partition_expression
FROM pg_partitions 
WHERE schemaname = 'partition'
UNION ALL
SELECT 
    'partition' as schemaname,
    'customers' as tablename,
    NULL as partitioned_table,
    'none' as partition_strategy,
    NULL as partition_expression;

-- =======================================================================================
-- NOTE: Hash partitioning example (commented out as it requires PostgreSQL 11+)
-- =======================================================================================

-- Uncomment for PostgreSQL 11+ environments:
-- CREATE TABLE user_sessions (
--     session_id SERIAL,
--     user_id INTEGER NOT NULL,
--     session_data TEXT,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- ) PARTITION BY HASH (user_id);
-- 
-- CREATE TABLE user_sessions_0 PARTITION OF user_sessions
--     FOR VALUES WITH (modulus 4, remainder 0);
-- CREATE TABLE user_sessions_1 PARTITION OF user_sessions
--     FOR VALUES WITH (modulus 4, remainder 1);
-- CREATE TABLE user_sessions_2 PARTITION OF user_sessions
--     FOR VALUES WITH (modulus 4, remainder 2);
-- CREATE TABLE user_sessions_3 PARTITION OF user_sessions
--     FOR VALUES WITH (modulus 4, remainder 3);