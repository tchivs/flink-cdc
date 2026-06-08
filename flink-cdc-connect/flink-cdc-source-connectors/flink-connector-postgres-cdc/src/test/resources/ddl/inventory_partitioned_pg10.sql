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

-- PostgreSQL 10 variant of inventory_partitioned.sql.
--
-- PG 10 supports declarative partitioning (PARTITION BY LIST / RANGE / HASH) but does NOT
-- support primary key constraints on the partitioned parent (PG 11+ does). Child partitions can
-- still carry their own PKs, which the source uses as the representative split key while keeping
-- the logical table id routed to the parent.

-- Create the schema that we'll use to populate data and watch the effect in the WAL
DROP SCHEMA IF EXISTS inventory_partitioned_pg10 CASCADE;
CREATE SCHEMA inventory_partitioned_pg10;
SET search_path TO inventory_partitioned_pg10;

-- Note: no PRIMARY KEY on the parent because PG 10 rejects it. REPLICA IDENTITY FULL
-- allows UPDATE/DELETE WAL events to carry the full old-row tuple, which Flink-CDC can
-- still consume.
CREATE TABLE products (
  id SERIAL NOT NULL,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  description VARCHAR(512),
  weight FLOAT,
  country VARCHAR(20) NOT NULL
) PARTITION BY LIST(country);
ALTER SEQUENCE products_id_seq RESTART WITH 101;
ALTER TABLE products REPLICA IDENTITY FULL;

CREATE TABLE products_uk PARTITION OF products
    FOR VALUES IN ('uk');
ALTER TABLE products_uk ADD PRIMARY KEY (id, country);
ALTER TABLE products_uk REPLICA IDENTITY FULL;

CREATE TABLE products_us PARTITION OF products
    FOR VALUES IN ('us');
ALTER TABLE products_us ADD PRIMARY KEY (id, country);
ALTER TABLE products_us REPLICA IDENTITY FULL;

INSERT INTO products
VALUES (default,'scooter','Small 2-wheel scooter',3.14, 'us'),
       (default,'car battery','12V car battery',8.1, 'us'),
       (default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8, 'us'),
       (default,'hammer','12oz carpenter''s hammer',0.75, 'us'),
       (default,'hammer','14oz carpenter''s hammer',0.875, 'us'),
       (default,'hammer','16oz carpenter''s hammer',1.0, 'uk'),
       (default,'rocks','box of assorted rocks',5.3, 'uk'),
       (default,'jacket','water resistent black wind breaker',0.1, 'uk'),
       (default,'spare tire','24 inch spare tire',22.2, 'uk');
