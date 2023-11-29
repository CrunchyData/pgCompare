CREATE SCHEMA confero AUTHORIZATION postgres;

set search_path=confero;

-- dc_result definition
CREATE TABLE dc_result (
	compare_dt timestamp NULL,
	table_name varchar(30) NULL,
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
CREATE UNLOGGED TABLE dc_source (
	table_name varchar(30) NULL,
	thread_nbr int4 NULL,
	batch_nbr int4 NULL,
	pk_hash varchar(40) NULL,
	column_hash varchar(40) NULL,
	pk jsonb NULL,
	compare_result bpchar(1) NULL
);

-- dc_target definition
CREATE UNLOGGED TABLE dc_target (
	table_name varchar(30) NULL,
	thread_nbr int4 NULL,
	batch_nbr int4 NULL,
	pk_hash varchar(40) NULL,
	column_hash varchar(40) NULL,
	pk jsonb NULL,
	compare_result bpchar(1) NULL
);

-- dc_table definition
CREATE TABLE dc_table (
	tid int8 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE),
	source_schema varchar(30) NULL,
	source_table varchar(30) NULL,
	target_schema varchar(30) NULL,
	target_table varchar(30) NULL,
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
	start_dt timestamp NOT NULL,
	end_dt timestamp NULL,
	load_id varchar(100) NULL,
	row_count int8 NULL,
	action_result jsonb NULL,
	batch_nbr int4 NOT NULL
);

CREATE INDEX dc_table_history_idx1 ON dc_table_history(tid, start_dt);
