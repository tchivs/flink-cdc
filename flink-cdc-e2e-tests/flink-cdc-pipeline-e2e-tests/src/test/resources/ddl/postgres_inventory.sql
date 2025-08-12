-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Create the schema that we'll use to populate data and watch the effect in the WAL
DROP SCHEMA IF EXISTS inventory CASCADE;
CREATE SCHEMA inventory;
SET search_path TO inventory;

-- Create and populate our products using a single insert with many rows
CREATE TABLE products (
                          id SERIAL NOT NULL PRIMARY KEY,
                          name VARCHAR(255) NOT NULL,
                          description VARCHAR(512),
                          weight FLOAT(24)
);
ALTER SEQUENCE products_id_seq RESTART WITH 101;
ALTER TABLE products REPLICA IDENTITY FULL;

INSERT INTO products
VALUES (default,'scooter','Small 2-wheel scooter',3.14),
       (default,'car battery','12V car battery',8.1),
       (default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8),
       (default,'hammer','12oz carpenter''s hammer',0.75),
       (default,'hammer','14oz carpenter''s hammer',0.875),
       (default,'hammer','16oz carpenter''s hammer',1.0),
       (default,'rocks','box of assorted rocks',5.3),
       (default,'jacket','water resistent black wind breaker',0.1),
       (default,'spare tire','24 inch spare tire',22.2);

-- Create customers table
CREATE TABLE customers (
                           id SERIAL PRIMARY KEY,
                           first_name VARCHAR(255) NOT NULL,
                           last_name VARCHAR(255) NOT NULL,
                           email VARCHAR(255) NOT NULL UNIQUE
);

ALTER SEQUENCE customers_id_seq RESTART WITH 101;
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Insert data into customers table
INSERT INTO customers (first_name, last_name, email)
VALUES ('Sally', 'Thomas', 'sally.thomas@acme.com'),
       ('George', 'Bailey', 'gbailey@foobar.com'),
       ('Edward', 'Walker', 'ed@walker.com'),
       ('Anne', 'Kretchmar', 'annek@noanswer.org');

-- ============================================================================
-- Partition Table Test Cases for Parent Table Discovery Bug Fix
-- ============================================================================

-- Create partitioned orders table (Range Partitioned by order_date)
CREATE TABLE orders (
    id SERIAL,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    PRIMARY KEY (id, order_date)
) PARTITION BY RANGE (order_date);

ALTER TABLE orders REPLICA IDENTITY FULL;

-- Create quarterly partitions for orders
CREATE TABLE orders_2023_q4 PARTITION OF orders
    FOR VALUES FROM ('2023-10-01') TO ('2024-01-01');
    
CREATE TABLE orders_2024_q1 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
    
CREATE TABLE orders_2024_q2 PARTITION OF orders
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');

ALTER TABLE orders_2023_q4 REPLICA IDENTITY FULL;
ALTER TABLE orders_2024_q1 REPLICA IDENTITY FULL;
ALTER TABLE orders_2024_q2 REPLICA IDENTITY FULL;

-- Insert test data into partitioned orders table
INSERT INTO orders (customer_id, order_date, product_id, quantity, total_amount, status)
VALUES 
    (101, '2023-11-15', 101, 2, 199.99, 'COMPLETED'),
    (102, '2023-12-20', 102, 1, 89.50, 'SHIPPED'),
    (103, '2024-01-10', 103, 3, 150.75, 'PENDING'),
    (101, '2024-02-14', 101, 1, 99.99, 'COMPLETED'),
    (104, '2024-03-18', 104, 2, 299.98, 'PROCESSING'),
    (102, '2024-05-22', 105, 1, 45.99, 'SHIPPED');

-- Create partitioned inventory table (List Partitioned by region)
CREATE TABLE inventory (
    id SERIAL,
    product_id INTEGER NOT NULL,
    warehouse_location VARCHAR(50) NOT NULL,
    region VARCHAR(20) NOT NULL,
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, region)
) PARTITION BY LIST (region);

ALTER TABLE inventory REPLICA IDENTITY FULL;

-- Create region-based partitions
CREATE TABLE inventory_north PARTITION OF inventory
    FOR VALUES IN ('north', 'northeast', 'northwest');
    
CREATE TABLE inventory_south PARTITION OF inventory  
    FOR VALUES IN ('south', 'southeast', 'southwest');
    
CREATE TABLE inventory_central PARTITION OF inventory
    FOR VALUES IN ('central', 'midwest');

ALTER TABLE inventory_north REPLICA IDENTITY FULL;
ALTER TABLE inventory_south REPLICA IDENTITY FULL;
ALTER TABLE inventory_central REPLICA IDENTITY FULL;

-- Insert test data into partitioned inventory table
INSERT INTO inventory (product_id, warehouse_location, region, stock_quantity)
VALUES 
    (101, 'Seattle-WH', 'northwest', 150),
    (102, 'Boston-WH', 'northeast', 200),
    (103, 'Miami-WH', 'southeast', 75),
    (104, 'Dallas-WH', 'south', 300),
    (105, 'Chicago-WH', 'midwest', 125),
    (101, 'Denver-WH', 'central', 180);

-- Create hash partitioned user_sessions table  
CREATE TABLE user_sessions (
    id SERIAL,
    user_id BIGINT NOT NULL,
    session_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_end TIMESTAMP,
    page_views INTEGER DEFAULT 0,
    duration_minutes INTEGER,
    PRIMARY KEY (id, user_id)
) PARTITION BY HASH (user_id);

ALTER TABLE user_sessions REPLICA IDENTITY FULL;

-- Create hash partitions
CREATE TABLE user_sessions_p0 PARTITION OF user_sessions
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);
    
CREATE TABLE user_sessions_p1 PARTITION OF user_sessions
    FOR VALUES WITH (MODULUS 4, REMAINDER 1);
    
CREATE TABLE user_sessions_p2 PARTITION OF user_sessions
    FOR VALUES WITH (MODULUS 4, REMAINDER 2);
    
CREATE TABLE user_sessions_p3 PARTITION OF user_sessions
    FOR VALUES WITH (MODULUS 4, REMAINDER 3);

ALTER TABLE user_sessions_p0 REPLICA IDENTITY FULL;
ALTER TABLE user_sessions_p1 REPLICA IDENTITY FULL;
ALTER TABLE user_sessions_p2 REPLICA IDENTITY FULL;
ALTER TABLE user_sessions_p3 REPLICA IDENTITY FULL;

-- Insert test data into hash partitioned table
INSERT INTO user_sessions (user_id, session_start, page_views, duration_minutes)
VALUES 
    (1001, '2024-01-15 10:30:00', 15, 25),
    (1002, '2024-01-15 11:45:00', 8, 12),
    (1003, '2024-01-15 14:20:00', 22, 35),
    (1004, '2024-01-15 16:10:00', 5, 8),
    (1005, '2024-01-15 18:30:00', 18, 28);

-- Regular table for comparison (non-partitioned)
CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation VARCHAR(20) NOT NULL,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details TEXT
);

ALTER TABLE audit_log REPLICA IDENTITY FULL;

-- Insert some audit log data
INSERT INTO audit_log (table_name, operation, changed_by, details)
VALUES 
    ('orders', 'INSERT', 'system', 'New order created'),
    ('inventory', 'UPDATE', 'admin', 'Stock quantity updated'),
    ('user_sessions', 'INSERT', 'system', 'User session started');