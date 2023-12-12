CREATE SCHEMA confero AUTHORIZATION postgres;

set search_path=confero;

-- dc_result definition
CREATE TABLE dc_result (
	compare_dt timestamptz NULL,
	table_name text NULL,
	equal_cnt int4 NULL,
	missing_source_cnt int4 NULL,
	missing_target_cnt int4 NULL,
	not_equal_cnt int4 NULL,
	source_cnt int4 NULL,
	target_cnt int4 NULL,
	cid serial4 NOT NULL,
	status varchar NULL,
	rid numeric null,
	CONSTRAINT dc_result_pk PRIMARY KEY (cid)
);


CREATE INDEX dc_result_idx1 ON dc_result(table_name, compare_dt);


-- dc_source definition
CREATE TABLE dc_source (
	table_name text NULL,
	thread_nbr int4 NULL,
	batch_nbr int4 NULL,
	pk_hash varchar(100) NULL,
	column_hash varchar(100) NULL,
	pk jsonb NULL,
	compare_result bpchar(1) NULL
);

-- dc_target definition
CREATE TABLE dc_target (
	table_name text NULL,
	thread_nbr int4 NULL,
	batch_nbr int4 NULL,
	pk_hash varchar(100) NULL,
	column_hash varchar(100) NULL,
	pk jsonb NULL,
	compare_result bpchar(1) NULL
);

-- dc_table definition
CREATE TABLE dc_table (
	tid int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	source_schema text NULL,
	source_table text NULL,
	target_schema text NULL,
	target_table text NULL,
	table_filter varchar(100) NULL,
	parallel_degree int4 NULL DEFAULT 1,
	status varchar(10) NULL DEFAULT 'disabled'::character varying,
	batch_nbr int4 NULL DEFAULT 1,
	mod_column varchar(200) NULL
);

ALTER TABLE dc_table ADD CONSTRAINT dc_table_pk PRIMARY KEY (tid);

-- dc_table_history definition
CREATE TABLE dc_table_history (
	tid int8 NOT NULL,
	action_type varchar(20) NOT NULL,
	start_dt timestamptz NOT NULL,
	end_dt timestamptz NULL,
	load_id varchar(100) NULL,
	row_count int8 NULL,
	action_result jsonb NULL,
	batch_nbr int4 NOT NULL
);

CREATE INDEX dc_table_history_idx1 ON dc_table_history(tid, start_dt);
