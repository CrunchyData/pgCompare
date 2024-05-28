CREATE SCHEMA pgcompare AUTHORIZATION postgres;

set search_path=pgcompare;

-- dc_table definition
CREATE TABLE dc_table (
	tid int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
    source_schema text NULL,
    source_table text NULL,
    target_schema text NULL,
    target_table text NULL,
    batch_nbr int4 NULL DEFAULT 1,
    parallel_degree int4 NULL DEFAULT 1,
    mod_column varchar(200) NULL,
    status varchar(10) NULL DEFAULT 'disabled'::character varying,
    table_filter varchar(100) NULL,
    column_map jsonb,
    column_info jsonb
);

ALTER TABLE dc_table ADD CONSTRAINT dc_table_pk PRIMARY KEY (tid);

-- dc_table_history definition
CREATE TABLE dc_table_history (
	tid int8 NOT NULL,
    load_id varchar(100) NULL,
    batch_nbr int4 NOT NULL,
    start_dt timestamptz NOT NULL,
    end_dt timestamptz NULL,
    action_result jsonb NULL,
    action_type varchar(20) NOT NULL,
    row_count int8 NULL
);

CREATE INDEX dc_table_history_idx1 ON dc_table_history(tid, start_dt);

-- dc_table definition
CREATE TABLE dc_object (
	oid int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_type text NULL,
    source_schema text NULL,
    source_object text NULL,
    target_schema text NULL,
    target_object text NULL,
    batch_nbr int4 NULL DEFAULT 1,
    status varchar(10) NULL DEFAULT 'review'::character varying,
    source_code text NULL,
    target_code text NULL
);

ALTER TABLE dc_object ADD CONSTRAINT dc_object_pk PRIMARY KEY (oid);

-- dc_result definition
CREATE TABLE dc_result (
        cid serial4 NOT NULL,
        rid numeric null,
        table_name text NULL,
        status varchar NULL,
        compare_dt timestamptz NULL,
        equal_cnt int4 NULL,
        missing_source_cnt int4 NULL,
        missing_target_cnt int4 NULL,
        not_equal_cnt int4 NULL,
        source_cnt int4 NULL,
        target_cnt int4 NULL,
	CONSTRAINT dc_result_pk PRIMARY KEY (cid)
);

CREATE INDEX dc_result_idx1 ON dc_result(table_name, compare_dt);

-- dc_source definition
CREATE TABLE dc_source (
    table_name text NULL,
    batch_nbr int4 NULL,
    pk jsonb NULL,
    pk_hash varchar(100) NULL,
    column_hash varchar(100) NULL,
    compare_result bpchar(1) NULL,
	thread_nbr int4 NULL
);

-- dc_target definition
CREATE TABLE dc_target (
    table_name text NULL,
    batch_nbr int4 NULL,
    pk jsonb NULL,
    pk_hash varchar(100) NULL,
    column_hash varchar(100) NULL,
    compare_result bpchar(1) NULL,
	thread_nbr int4 NULL
);


