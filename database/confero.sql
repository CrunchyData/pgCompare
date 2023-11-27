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
)
PARTITION BY RANGE (thread_nbr);

-- Partitions
CREATE UNLOGGED TABLE dc_source_t0 PARTITION OF dc_source  FOR VALUES FROM (0) TO (1) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_source_t1 PARTITION OF dc_source  FOR VALUES FROM (1) TO (2) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_source_t2 PARTITION OF dc_source  FOR VALUES FROM (2) TO (3) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_source_t3 PARTITION OF dc_source  FOR VALUES FROM (3) TO (4) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_source_t4 PARTITION OF dc_source  FOR VALUES FROM (4) TO (5) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_source_t5 PARTITION OF dc_source  FOR VALUES FROM (5) TO (6) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_source_t6 PARTITION OF dc_source  FOR VALUES FROM (6) TO (7) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_source_t7 PARTITION OF dc_source  FOR VALUES FROM (7) TO (8) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_source_t8 PARTITION OF dc_source  FOR VALUES FROM (8) TO (9) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_source_t9 PARTITION OF dc_source  FOR VALUES FROM (9) TO (99) with (autovacuum_enabled=false);

-- dc_target definition
CREATE UNLOGGED TABLE dc_target (
	table_name varchar(30) NULL,
	thread_nbr int4 NULL,
	batch_nbr int4 NULL,
	pk_hash varchar(40) NULL,
	column_hash varchar(40) NULL,
	pk jsonb NULL,
	compare_result bpchar(1) NULL
)
PARTITION BY RANGE (thread_nbr);

-- Partitions
CREATE UNLOGGED TABLE dc_target_t0 PARTITION OF dc_target  FOR VALUES FROM (0) TO (1) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_target_t1 PARTITION OF dc_target  FOR VALUES FROM (1) TO (2) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_target_t2 PARTITION OF dc_target  FOR VALUES FROM (2) TO (3) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_target_t3 PARTITION OF dc_target  FOR VALUES FROM (3) TO (4) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_target_t4 PARTITION OF dc_target  FOR VALUES FROM (4) TO (5) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_target_t5 PARTITION OF dc_target  FOR VALUES FROM (5) TO (6) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_target_t6 PARTITION OF dc_target  FOR VALUES FROM (6) TO (7) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_target_t7 PARTITION OF dc_target  FOR VALUES FROM (7) TO (8) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_target_t8 PARTITION OF dc_target  FOR VALUES FROM (8) TO (9) with (autovacuum_enabled=false);
CREATE UNLOGGED TABLE dc_target_t9 PARTITION OF dc_target  FOR VALUES FROM (9) TO (99) with (autovacuum_enabled=false);

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
