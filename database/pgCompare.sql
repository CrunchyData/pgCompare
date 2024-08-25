CREATE SCHEMA pgcompare AUTHORIZATION postgres;

set search_path=pgcompare;

CREATE TABLE dc_project (
pid            int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
project_name   text NOT NULL default 'default',
project_config text NULL);

ALTER TABLE dc_project ADD CONSTRAINT dc_project_pk PRIMARY KEY (pid);

INSERT INTO dc_project (pid, project_name) VALUES (1, 'default');

-- dc_table definition
CREATE TABLE dc_table (
    pid                  int8 NOT NULL default 1,
	tid                  int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	table_alias          text NULL,
    status               varchar(10) NULL DEFAULT 'disabled'::character varying,
    batch_nbr            int4 NULL DEFAULT 1
);

ALTER TABLE dc_table ADD CONSTRAINT dc_table_pk PRIMARY KEY (tid);

-- dc_table_column definition
CREATE TABLE dc_table_column (
  tid                  int8 NOT NULL,
  column_alias         varchar(50) NOT NULL,
  column_type          varchar(10) DEFAULT 'source' NOT NULL,
  column_name          varchar(50) NULL,
  data_type            varchar(20) NOT NULL,
  data_class           varchar(20) DEFAULT 'string',
  data_length          int,
  number_precision     int,
  number_scale         int,
  column_nullable      boolean DEFAULT true,
  column_primarykey    boolean DEFAULT false,
  map_expression       varchar(500),
  supported            boolean DEFAULT true,
  preserve_case        boolean DEFAULT false
)

ALTER TABLE dc_table_column ADD CONSTRAINT dc_table_column_pk PRIMARY KEY (tid, column_alias, column_type);

-- dc_table_history definition
CREATE TABLE dc_table_history (
	tid              int8 NOT NULL,
    load_id          varchar(100) NULL,
    batch_nbr        int4 NOT NULL,
    start_dt         timestamptz NOT NULL,
    end_dt           timestamptz NULL,
    action_result    jsonb NULL,
    action_type      varchar(20) NOT NULL,
    row_count        int8 NULL
);

CREATE INDEX dc_table_history_idx1 ON dc_table_history(tid, start_dt);


CREATE TABLE dc_table_map  (
    tid                   int8 NOT NULL,
    dest_type             varchar(20) DEFAULT 'target' NOT NULL,
    schema_name           text NOT NULL,
    table_name            text NOT NULL,
    parallel_degree       int DEFAULT 1,
    mod_column            varchar(200),
    table_filter          varchar(200),
    schema_preserve_case  boolean DEFAULT false,
    table_preserve_case   boolean DEFAULT false
);

ALTER TABLE dc_table_map ADD CONSTRAINT dc_table_map_pk PRIMARY KEY (tid, dest_type, schema_name, table_name);


-- dc_result definition
CREATE TABLE dc_result (
        cid                   serial4 NOT NULL,
        rid                   numeric null,
        tid                   int8 null,
        table_name            text NULL,
        status                varchar NULL,
        compare_dt            timestamptz NULL,
        equal_cnt             int4 NULL,
        missing_source_cnt    int4 NULL,
        missing_target_cnt    int4 NULL,
        not_equal_cnt         int4 NULL,
        source_cnt            int4 NULL,
        target_cnt            int4 NULL,
	CONSTRAINT dc_result_pk PRIMARY KEY (cid)
);

CREATE INDEX dc_result_idx1 ON dc_result(table_name, compare_dt);

-- dc_source definition
CREATE TABLE dc_source (
    tid                       int8 null,
    table_name                text NULL,
    batch_nbr                 int4 NULL,
    pk                        jsonb NULL,
    pk_hash                   varchar(100) NULL,
    column_hash               varchar(100) NULL,
    compare_result            bpchar(1) NULL,
	thread_nbr                int4 NULL
);

-- dc_target definition
CREATE TABLE dc_target (
    tid                      int8 null,
    table_name               text NULL,
    batch_nbr                int4 NULL,
    pk                       jsonb NULL,
    pk_hash                  varchar(100) NULL,
    column_hash              varchar(100) NULL,
    compare_result           bpchar(1) NULL,
	thread_nbr               int4 NULL
);

ALTER TABLE dc_table_column ADD CONSTRAINT dc_table_column_dc_table_fk FOREIGN KEY (tid) REFERENCES dc_table(tid) ON DELETE CASCADE;
ALTER TABLE dc_table_map    ADD CONSTRAINT dc_table_map_dc_table_fk    FOREIGN KEY (tid) REFERENCES dc_table(tid) ON DELETE CASCADE;


