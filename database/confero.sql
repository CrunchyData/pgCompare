CREATE SCHEMA confero AUTHORIZATION postgres;

set search_path=confero;

-- dc_result definition
CREATE TABLE dc_result (
	cid serial4 NOT NULL,
	compare_dt timestamptz NULL,
	equal_cnt int4 NULL,
	missing_source_cnt int4 NULL,
	missing_target_cnt int4 NULL,
	not_equal_cnt int4 NULL,
	rid numeric null,
	status varchar NULL,
	source_cnt int4 NULL,
	table_name text NULL,
	target_cnt int4 NULL,
	CONSTRAINT dc_result_pk PRIMARY KEY (cid)
);

CREATE INDEX dc_result_idx1 ON dc_result(table_name, compare_dt);

-- dc_source definition
CREATE TABLE dc_source (
	batch_nbr int4 NULL,
	column_hash varchar(100) NULL,
	compare_result bpchar(1) NULL,
	pk_hash varchar(100) NULL,
	pk jsonb NULL,
	table_name text NULL,
	thread_nbr int4 NULL
);

-- dc_target definition
CREATE TABLE dc_target (
	batch_nbr int4 NULL,
	column_hash varchar(100) NULL,
	compare_result bpchar(1) NULL,
	pk_hash varchar(100) NULL,
	pk jsonb NULL,
	table_name text NULL,
	thread_nbr int4 NULL
);

-- dc_table definition
CREATE TABLE dc_table (
	tid int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	batch_nbr int4 NULL DEFAULT 1,
	column_map jsonb,
	mod_column varchar(200) NULL
	parallel_degree int4 NULL DEFAULT 1,
	status varchar(10) NULL DEFAULT 'disabled'::character varying,
	source_schema text NULL,
	source_table text NULL,
	table_filter varchar(100) NULL,
	target_schema text NULL,
	target_table text NULL
);

ALTER TABLE dc_table ADD CONSTRAINT dc_table_pk PRIMARY KEY (tid);

-- dc_table_history definition
CREATE TABLE dc_table_history (
	tid int8 NOT NULL,
	action_result jsonb NULL,
	action_type varchar(20) NOT NULL,
	batch_nbr int4 NOT NULL,
	end_dt timestamptz NULL,
	load_id varchar(100) NULL,
	row_count int8 NULL,
	start_dt timestamptz NOT NULL
);

CREATE INDEX dc_table_history_idx1 ON dc_table_history(tid, start_dt);
