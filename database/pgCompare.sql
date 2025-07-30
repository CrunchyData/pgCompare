CREATE SCHEMA pgcompare AUTHORIZATION postgres;

set search_path=pgcompare;

-- DROP TABLE dc_project;

CREATE TABLE dc_project (
	pid int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	project_name text DEFAULT 'default'::text NOT NULL,
	project_config jsonb NULL,
	CONSTRAINT dc_project_pk PRIMARY KEY (pid)
);

-- DROP TABLE dc_result;

CREATE TABLE dc_result (
	cid serial4 NOT NULL,
	rid numeric NULL,
	tid int8 NULL,
	table_name text NULL,
	status varchar NULL,
	compare_start timestamptz NULL,
	equal_cnt int4 NULL,
	missing_source_cnt int4 NULL,
	missing_target_cnt int4 NULL,
	not_equal_cnt int4 NULL,
	source_cnt int4 NULL,
	target_cnt int4 NULL,
	compare_end timestamptz NULL,
	CONSTRAINT dc_result_pk PRIMARY KEY (cid)
);

-- DROP TABLE dc_source;

CREATE TABLE dc_source (
	tid int8 NULL,
	table_name text NULL,
	batch_nbr int4 NULL,
	pk jsonb NULL,
	pk_hash varchar(100) NULL,
	column_hash varchar(100) NULL,
	compare_result bpchar(1) NULL,
	thread_nbr int4 NULL
);

-- DROP TABLE dc_table;

CREATE TABLE dc_table (
	pid int8 DEFAULT 1 NOT NULL,
	tid int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	table_alias text NULL,
    enabled boolean default true,
	--status varchar(10) DEFAULT 'disabled'::character varying NULL,
	batch_nbr int4 DEFAULT 1 NULL,
	parallel_degree int4 DEFAULT 1 NULL,
	CONSTRAINT dc_table_pk PRIMARY KEY (tid)
);

-- DROP TABLE dc_table_column;

CREATE TABLE dc_table_column (
	tid int8 NOT NULL,
	column_id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	column_alias text NOT NULL,
    enabled boolean default true,
	CONSTRAINT dc_table_column_pk PRIMARY KEY (column_id)
);


-- DROP TABLE dc_table_column_map;

CREATE TABLE dc_table_column_map (
	tid int8 NOT NULL,
	column_id int8 NOT NULL,
	column_origin varchar(10) DEFAULT 'source'::character varying NOT NULL,
	column_name text NOT NULL,
	data_type text NOT NULL,
	data_class varchar(20) DEFAULT 'string'::character varying NULL,
	data_length int4 NULL,
	number_precision int4 NULL,
	number_scale int4 NULL,
	column_nullable bool DEFAULT true NULL,
	column_primarykey bool DEFAULT false NULL,
	map_expression text NULL,
	supported bool DEFAULT true NULL,
	preserve_case bool DEFAULT false NULL,
	map_type varchar(15) DEFAULT 'column'::character varying NOT NULL,
	CONSTRAINT dc_table_column_map_pk PRIMARY KEY (column_id, column_origin, column_name)
);

-- DROP TABLE dc_table_history;

CREATE TABLE dc_table_history (
	tid int8 NOT NULL,
	batch_nbr int4 NOT NULL,
	start_dt timestamptz NOT NULL,
	end_dt timestamptz NULL,
	action_result jsonb NULL,
	row_count int8 NULL
);

-- DROP TABLE dc_table_map;

CREATE TABLE dc_table_map (
	tid int8 NOT NULL,
	dest_type varchar(20) DEFAULT 'target'::character varying NOT NULL,
	schema_name text NOT NULL,
	table_name text NOT NULL,
	mod_column varchar(200) NULL,
	table_filter varchar(200) NULL,
	schema_preserve_case bool DEFAULT false NULL,
	table_preserve_case bool DEFAULT false NULL,
	CONSTRAINT dc_table_map_pk PRIMARY KEY (tid, dest_type, schema_name, table_name)
);

-- DROP TABLE dc_target;

CREATE TABLE dc_target (
	tid int8 NULL,
	table_name text NULL,
	batch_nbr int4 NULL,
	pk jsonb NULL,
	pk_hash varchar(100) NULL,
	column_hash varchar(100) NULL,
	compare_result bpchar(1) NULL,
	thread_nbr int4 NULL
);


--
-- Indexes
--

CREATE INDEX dc_result_idx1 ON dc_result USING btree (table_name, compare_start);
CREATE INDEX dc_table_history_idx1 ON dc_table_history USING btree (tid, start_dt);
CREATE INDEX dc_table_idx1 ON dc_table USING btree (table_alias);
CREATE INDEX dc_table_column_idx1 ON dc_table_column USING btree (column_alias, tid, column_id);

--
-- Foreign Keys
--
ALTER TABLE dc_table_column ADD CONSTRAINT dc_table_column_fk FOREIGN KEY (tid) REFERENCES dc_table(tid) ON DELETE CASCADE;
ALTER TABLE dc_table_column_map ADD CONSTRAINT dc_table_column_map_fk FOREIGN KEY (column_id) REFERENCES dc_table_column(column_id) ON DELETE CASCADE;
ALTER TABLE dc_table_map ADD CONSTRAINT dc_table_map_fk FOREIGN KEY (tid) REFERENCES dc_table(tid) ON DELETE CASCADE;

--
-- Data
--
INSERT INTO pgcompare.dc_project (project_name,project_config) VALUES
	 ('default',NULL);

--
-- Functions
--
-- DROP FUNCTION pgcompare.dc_copy_table(int4, int4);

CREATE OR REPLACE FUNCTION pgcompare.dc_copy_table(p_pid integer, p_tid integer)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
v_new_tid bigint;
    v_new_cid bigint;
    r_column record;
BEGIN
    -- Duplicate dc_table
INSERT INTO dc_table (pid, tid, table_alias, enabled, batch_nbr, parallel_degree)
SELECT pid, tid, table_alias, enabled, batch_nbr, parallel_degree
FROM dc_table
WHERE pid = p_pid AND tid = p_tid
    RETURNING tid INTO v_new_tid;

-- Duplicate dc_table_map
INSERT INTO dc_table_map (tid, dest_type, schema_name, table_name, mod_column, table_filter, schema_preserve_case, table_preserve_case)
SELECT v_new_tid, dest_type, schema_name, table_name, mod_column, table_filter, schema_preserve_case, table_preserve_case
FROM dc_table_map
WHERE tid = p_tid;

-- Duplicate dc_table_column and dc_table_column_map
FOR r_column IN
SELECT tid, column_id, column_alias, enabled
FROM dc_table_column
WHERE tid = p_tid
    LOOP
-- Insert into dc_table_column with new tid and potentially new cid
INSERT INTO dc_table_column (tid, column_id, column_alias, enabled)
SELECT v_new_tid, cid, tid, column_id, column_alias, enabled
FROM dc_table_column
WHERE tid = p_tid
  AND column_id = r_column.column_id
    RETURNING cid INTO v_new_cid;

-- Duplicate dc_table_column_map
INSERT INTO dc_table_column_map (tid, column_id, column_origin, column_name, data_type, data_class, data_length, number_precision,
                                 number_scale, column_nullable, column_primarykey, map_expression, supported, preserve_case, map_type)
SELECT v_new_tid, v_new_cid, column_origin, column_name, data_type, data_class, data_length, number_precision,
       number_scale, column_nullable, column_primarykey, map_expression, supported, preserve_case, map_type
FROM dc_table_column_map
WHERE tid = p_tid
  AND column_id = r_column.column_id;
END LOOP;

RETURN v_new_tid;
END;
$function$
;
