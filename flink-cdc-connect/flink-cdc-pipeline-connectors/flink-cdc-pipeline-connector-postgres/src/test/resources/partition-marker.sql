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

-- Partition marker prerequisite of the PostgreSQL 10/11/12 partition routing, as installed in
-- production by the CDC prerequisite check of flink-manager. Needs a superuser (event triggers);
-- afterwards add the table to the publication and grant SELECT on it to the CDC user.
CREATE TABLE IF NOT EXISTS public.flink_cdc_partition_marker (
    id bigserial PRIMARY KEY,
    lsn bigint NOT NULL,
    parent text NOT NULL,
    children text[]
);

CREATE OR REPLACE FUNCTION public.flink_cdc_partition_marker_write(parent_oid oid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('flink_cdc_partition_marker'), parent_oid::int);
    INSERT INTO public.flink_cdc_partition_marker (lsn, parent, children)
    SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint, n.nspname || '.' || c.relname,
           ARRAY(SELECT cn.nspname || '.' || cc.relname
                 FROM pg_inherits i
                 JOIN pg_class cc ON cc.oid = i.inhrelid
                 JOIN pg_namespace cn ON cn.oid = cc.relnamespace
                 WHERE i.inhparent = c.oid AND cc.relispartition
                 ORDER BY 1)
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = parent_oid AND c.relkind = 'p';
END $$;

CREATE OR REPLACE FUNCTION public.flink_cdc_partition_marker_ddl()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    target oid;
BEGIN
    FOR target IN
        SELECT DISTINCT t.oid FROM (
            SELECT d.objid AS oid FROM pg_event_trigger_ddl_commands() d
            WHERE d.classid = 'pg_class'::regclass AND d.objsubid = 0
            UNION ALL
            SELECT i.inhparent FROM pg_event_trigger_ddl_commands() d
            JOIN pg_inherits i ON i.inhrelid = d.objid
            JOIN pg_class c ON c.oid = d.objid
            WHERE d.classid = 'pg_class'::regclass AND c.relispartition
        ) t
    LOOP
        PERFORM public.flink_cdc_partition_marker_write(target);
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION public.flink_cdc_partition_marker_drop()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    INSERT INTO public.flink_cdc_partition_marker (lsn, parent, children)
    SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint, d.schema_name || '.' || d.object_name, NULL
    FROM pg_event_trigger_dropped_objects() d
    WHERE d.object_type = 'table';
END $$;

DROP EVENT TRIGGER IF EXISTS flink_cdc_partition_marker_ddl;
CREATE EVENT TRIGGER flink_cdc_partition_marker_ddl ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE')
    EXECUTE PROCEDURE public.flink_cdc_partition_marker_ddl();

DROP EVENT TRIGGER IF EXISTS flink_cdc_partition_marker_drop;
CREATE EVENT TRIGGER flink_cdc_partition_marker_drop ON sql_drop
    WHEN TAG IN ('DROP TABLE')
    EXECUTE PROCEDURE public.flink_cdc_partition_marker_drop();

-- Baseline: the partitions of every partitioned table as of now.
SELECT public.flink_cdc_partition_marker_write(c.oid) FROM pg_class c WHERE c.relkind = 'p';
