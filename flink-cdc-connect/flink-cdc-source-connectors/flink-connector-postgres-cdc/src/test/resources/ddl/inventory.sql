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
  weight FLOAT
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

-- ============================================================================
-- Partition Table Test Cases for Parent Table Discovery Bug Fix
-- ============================================================================

-- Create partitioned product_sales table (Range Partitioned by sale_date)
CREATE TABLE product_sales (
    id SERIAL,
    product_id INTEGER NOT NULL,
    sale_date DATE NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    customer_region VARCHAR(20),
    PRIMARY KEY (id, sale_date)
) PARTITION BY RANGE (sale_date);

ALTER TABLE product_sales REPLICA IDENTITY FULL;

-- Create monthly partitions for product_sales
CREATE TABLE product_sales_202401 PARTITION OF product_sales
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    
CREATE TABLE product_sales_202402 PARTITION OF product_sales
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
    
CREATE TABLE product_sales_202403 PARTITION OF product_sales
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

ALTER TABLE product_sales_202401 REPLICA IDENTITY FULL;
ALTER TABLE product_sales_202402 REPLICA IDENTITY FULL;
ALTER TABLE product_sales_202403 REPLICA IDENTITY FULL;

-- Insert test data into partitioned product_sales table
INSERT INTO product_sales (product_id, sale_date, quantity, unit_price, customer_region)
VALUES 
    (101, '2024-01-15', 5, 19.99, 'north'),
    (102, '2024-01-20', 2, 45.50, 'south'),
    (103, '2024-02-10', 8, 12.75, 'east'),
    (101, '2024-02-18', 3, 19.99, 'west'),
    (104, '2024-03-05', 6, 33.99, 'north'),
    (105, '2024-03-25', 4, 27.50, 'central');

-- Create partitioned warehouse_inventory table (List Partitioned by warehouse_type)
CREATE TABLE warehouse_inventory (
    id SERIAL,
    product_id INTEGER NOT NULL,
    warehouse_type VARCHAR(20) NOT NULL,
    location_code VARCHAR(10) NOT NULL,
    current_stock INTEGER NOT NULL DEFAULT 0,
    reorder_level INTEGER NOT NULL DEFAULT 10,
    last_restocked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, warehouse_type)
) PARTITION BY LIST (warehouse_type);

ALTER TABLE warehouse_inventory REPLICA IDENTITY FULL;

-- Create warehouse type-based partitions
CREATE TABLE warehouse_inventory_main PARTITION OF warehouse_inventory
    FOR VALUES IN ('main', 'primary', 'central');
    
CREATE TABLE warehouse_inventory_satellite PARTITION OF warehouse_inventory  
    FOR VALUES IN ('satellite', 'remote', 'branch');
    
CREATE TABLE warehouse_inventory_backup PARTITION OF warehouse_inventory
    FOR VALUES IN ('backup', 'reserve', 'overflow');

ALTER TABLE warehouse_inventory_main REPLICA IDENTITY FULL;
ALTER TABLE warehouse_inventory_satellite REPLICA IDENTITY FULL;
ALTER TABLE warehouse_inventory_backup REPLICA IDENTITY FULL;

-- Insert test data into partitioned warehouse_inventory table
INSERT INTO warehouse_inventory (product_id, warehouse_type, location_code, current_stock, reorder_level)
VALUES 
    (101, 'main', 'WH001', 250, 50),
    (102, 'central', 'WH002', 180, 30),
    (103, 'satellite', 'SAT01', 95, 20),
    (104, 'remote', 'RMT01', 120, 25),
    (105, 'backup', 'BCK01', 300, 60),
    (101, 'reserve', 'RSV01', 75, 15);