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
                          name VARCHAR(255) NOT NULL DEFAULT 'flink',
                          description VARCHAR(512),
                          weight FLOAT(38)
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

ALTER SEQUENCE customers_id_seq RESTART WITH 1001;
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Insert data into customers table
INSERT INTO customers (first_name, last_name, email)
VALUES ('Sally', 'Thomas', 'sally.thomas@acme.com'),
       ('George', 'Bailey', 'gbailey@foobar.com'),
       ('Edward', 'Walker', 'ed@walker.com'),
       ('Anne', 'Kretchmar', 'annek@noanswer.org');

-- Create orders table
CREATE TABLE orders (
                        order_number SERIAL PRIMARY KEY,
                        order_date DATE NOT NULL,
                        purchaser INTEGER NOT NULL,
                        quantity INTEGER NOT NULL,
                        product_id INTEGER NOT NULL,
                        CONSTRAINT fk_customer FOREIGN KEY (purchaser) REFERENCES customers(id),
                        CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Insert data into orders table
INSERT INTO orders (order_date, purchaser, quantity, product_id)
VALUES ('2016-01-16', 1001, 1, 102),
       ('2016-01-17', 1002, 2, 105),
       ('2016-02-18', 1004, 3, 109),
       ('2016-02-19', 1002, 2, 106),
       ('2016-02-21', 1003, 1, 107);

-- Create multi_max_table
CREATE TABLE multi_max_table (
                                 order_id VARCHAR(128) NOT NULL,
                                 index INT NOT NULL,
                                 description VARCHAR(512) NOT NULL,
                                 PRIMARY KEY (order_id, index)
);

-- Insert data into multi_max_table
INSERT INTO multi_max_table (order_id, index, description)
VALUES ('', 0, 'flink'),
       ('', 1, 'flink'),
       ('', 2, 'flink'),
       ('a', 0, 'flink'),
       ('b', 0, 'flink'),
       ('c', 0, 'flink'),
       ('d', 0, 'flink'),
       ('E', 0, 'flink'),
       ('E', 1, 'flink'),
       ('E', 2, 'flink'),
       ('e', 4, 'flink'),
       ('E', 3, 'flink');

-- ============================================================================
-- Partition Table Test Cases for Parent Table Discovery Bug Fix
-- ============================================================================

-- Create partitioned order_history table (Range Partitioned by order_date)
CREATE TABLE order_history (
                               id SERIAL ,
                               order_number INTEGER NOT NULL,
                               customer_id INTEGER NOT NULL,
                               order_date DATE NOT NULL,
                               total_amount DECIMAL(12,2) NOT NULL,
                               order_status VARCHAR(20) DEFAULT 'PENDING',
                               processed_at TIMESTAMP
) PARTITION BY RANGE (order_date);

ALTER TABLE order_history REPLICA IDENTITY FULL;
ALTER SEQUENCE order_history_id_seq RESTART WITH 1;

-- Create yearly partitions for order_history
CREATE TABLE order_history_2023 PARTITION OF order_history
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
ALTER TABLE order_history_2023 ADD PRIMARY KEY (id);

CREATE TABLE order_history_2024 PARTITION OF order_history
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
ALTER TABLE order_history_2024 ADD PRIMARY KEY (id);

ALTER TABLE order_history_2023 REPLICA IDENTITY FULL;
ALTER TABLE order_history_2024 REPLICA IDENTITY FULL;

-- Insert test data into partitioned order_history table
INSERT INTO order_history (order_number, customer_id, order_date, total_amount, order_status, processed_at)
VALUES
    (1001, 1001, '2023-06-15', 299.99, 'COMPLETED', '2023-06-15 10:30:00'),
    (1002, 1002, '2023-08-20', 145.75, 'SHIPPED', '2023-08-20 14:20:00'),
    (1003, 1003, '2024-01-10', 89.50, 'PROCESSING', NULL),
    (1004, 1001, '2024-03-18', 199.99, 'COMPLETED', '2024-03-18 16:45:00'),
    (1005, 1004, '2024-05-22', 349.99, 'SHIPPED', '2024-05-22 11:15:00');

-- Create partitioned customer_segments table (List Partitioned by segment)
CREATE TABLE customer_segments (
                                   id SERIAL,
                                   customer_id INTEGER NOT NULL,
                                   segment VARCHAR(20) NOT NULL,
                                   tier VARCHAR(10) NOT NULL,
                                   points INTEGER DEFAULT 0,
                                   joined_date DATE NOT NULL,
                                   last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY LIST (segment);

ALTER TABLE customer_segments REPLICA IDENTITY FULL;

-- Create segment-based partitions
CREATE TABLE customer_segments_premium PARTITION OF customer_segments
    FOR VALUES IN ('premium', 'vip', 'platinum');

CREATE TABLE customer_segments_standard PARTITION OF customer_segments
    FOR VALUES IN ('standard', 'regular', 'bronze');

CREATE TABLE customer_segments_basic PARTITION OF customer_segments
    FOR VALUES IN ('basic', 'starter', 'trial');
ALTER TABLE customer_segments_premium ADD PRIMARY KEY (id);
ALTER TABLE customer_segments_premium REPLICA IDENTITY FULL;
ALTER TABLE customer_segments_standard ADD PRIMARY KEY (id);
ALTER TABLE customer_segments_standard REPLICA IDENTITY FULL;
ALTER TABLE customer_segments_basic ADD PRIMARY KEY (id);
ALTER TABLE customer_segments_basic REPLICA IDENTITY FULL;

-- Insert test data into partitioned customer_segments table
INSERT INTO customer_segments (customer_id, segment, tier, points, joined_date)
VALUES
    (1001, 'premium', 'gold', 2500, '2023-01-15'),
    (1002, 'standard', 'silver', 1200, '2023-03-20'),
    (1003, 'basic', 'bronze', 350, '2024-01-10'),
    (1004, 'vip', 'platinum', 5000, '2022-12-05'),
    (1001, 'regular', 'silver', 800, '2023-06-18');
