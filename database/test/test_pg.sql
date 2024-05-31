-- pgc_core_types definition

-- Drop table

-- DROP TABLE pgc_core_types;

CREATE TABLE pgc_core_types (
	id int4 NOT NULL,
	col_int8 int2 NULL,
	col_int16 int2 NULL,
	col_int32 int4 NULL,
	col_int64 int8 NULL,
	col_dec_20 numeric(20) NULL,
	col_dec_38 numeric(38) NULL,
	col_dec_10_2 numeric(10, 2) NULL,
	col_float32 float4 NULL,
	col_float64 float8 NULL,
	col_varchar_30 varchar(30) NULL,
	col_char_2 bpchar(2) NULL,
	col_string text NULL,
	col_date date NULL,
	col_datetime timestamp(3) NULL,
	col_tstz timestamptz(3) NULL,
	CONSTRAINT pgc_core_types_pkey PRIMARY KEY (id)
);


-- pgc_large_decimals definition

-- Drop table

-- DROP TABLE pgc_large_decimals;

CREATE TABLE pgc_large_decimals (
	id numeric(38) NOT NULL,
	col_data varchar(10) NULL,
	col_dec_18 numeric(18) NULL,
	col_dec_38 numeric(38) NULL,
	col_dec_38_9 numeric(38, 9) NULL,
	col_dec_38_30 numeric(38, 30) NULL,
	CONSTRAINT pgc_large_decimals_pkey PRIMARY KEY (id)
);


-- pgc_null_not_null definition

-- Drop table

-- DROP TABLE pgc_null_not_null;

CREATE TABLE pgc_null_not_null (
	col_nn timestamp(0) NOT NULL,
	col_nullable timestamp(0) NULL,
	col_src_nn_trg_n timestamp(0) NOT NULL,
	col_src_n_trg_nn timestamp(0) NULL
);


-- pgc_ora2pg_types definition

-- Drop table

-- DROP TABLE pgc_ora2pg_types;

CREATE TABLE pgc_ora2pg_types (
	id int4 NOT NULL,
	col_num_4 int2 NULL,
	col_num_9 int4 NULL,
	col_num_18 int8 NULL,
	col_num_38 numeric(38) NULL,
	col_num numeric NULL,
	col_num_10_2 numeric(10, 2) NULL,
	col_num_float float4 NULL,
	col_float32 float4 NULL,
	col_float64 float8 NULL,
	col_varchar_30 varchar(30) NULL,
	col_char_2 bpchar(2) NULL,
	col_nvarchar_30 varchar(30) NULL,
	col_nchar_2 bpchar(2) NULL,
	col_date date NULL,
	col_ts timestamp(6) NULL,
	col_tstz timestamptz(6) NULL,
	col_raw bytea NULL,
	col_long_raw bytea NULL,
	col_blob bytea NULL,
	col_clob text NULL,
	col_nclob text NULL,
	CONSTRAINT pgc_ora2pg_types_pkey PRIMARY KEY (id)
);


-- pgc_pg_types definition

-- Drop table

-- DROP TABLE pgc_pg_types;

CREATE TABLE pgc_pg_types (
	id serial4 NOT NULL,
	col_int2 int2 NULL,
	col_int4 int4 NULL,
	col_int8 int8 NULL,
	col_dec numeric NULL,
	col_dec_10_2 numeric(10, 2) NULL,
	col_float32 float4 NULL,
	col_float64 float8 NULL,
	col_varchar_30 varchar(30) NULL,
	col_char_2 bpchar(2) NULL,
	col_text text NULL,
	col_date date NULL,
	col_ts timestamp(6) NULL,
	col_tstz timestamptz(6) NULL,
	col_time time(6) NULL,
	col_timetz timetz(6) NULL,
	col_binary bytea NULL,
	col_bool bool NULL,
	col_uuid uuid NULL,
	col_oid oid NULL,
	CONSTRAINT pgc_pg_types_pkey PRIMARY KEY (id)
);


-- pgc_generate_partitions definition

-- Drop table

-- DROP TABLE pgc_generate_partitions;

CREATE TABLE pgc_generate_partitions (
	course_id varchar(12) NOT NULL,
	quarter_id int4 NOT NULL,
	recd_timestamp timestamp NOT NULL,
	registration_date date NOT NULL,
	approved bool NOT NULL,
	grade numeric NULL,
	CONSTRAINT pgc_generate_partitions_pkey PRIMARY KEY (course_id, quarter_id, recd_timestamp, registration_date, approved)
);


--------------------------------------------------------
--  Data
--------------------------------------------------------

insert into PGC_CORE_TYPES (ID,COL_INT8,COL_INT16,COL_INT32,COL_INT64,COL_DEC_20,COL_DEC_38,COL_DEC_10_2,COL_FLOAT32,COL_FLOAT64,COL_VARCHAR_30,COL_CHAR_2,COL_STRING,COL_DATE,COL_DATETIME,COL_TSTZ) values
	 (2,2,2,2,2,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2,'Hello DVT','B ','Hello DVT','1970-01-02','1970-01-02 00:00:02','1970-01-01 21:00:02-05'),
	 (3,3,3,3,3,12345678901234567890,1234567890123456789012345,123.33,123456.3,12345678.3,'Hello DVT','C ','Hello DVT','1970-01-03','1970-01-03 00:00:03','1970-01-02 22:00:03-05'),
	 (1,1,1,1,1,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1,'Hello DVT','A ','Hello DVT','1970-01-01','1970-01-01 00:00:01','1969-12-31 20:00:01-05');

insert into PGC_LARGE_DECIMALS (ID,COL_DATA,COL_DEC_18,COL_DEC_38,COL_DEC_38_9,COL_DEC_38_30) values
	 (123456789012345678901234567890,'Row 1',123456789012345678,12345678901234567890123456789012345678,12345678901234567890123456789.123456789,12345678.123456789012345678901234567890),
	 (223456789012345678901234567890,'Row 2',223456789012345678,22345678901234567890123456789012345678,22345678901234567890123456789.123456789,22345678.123456789012345678901234567890),
	 (323456789012345678901234567890,'Row 3',323456789012345678,32345678901234567890123456789012345678,32345678901234567890123456789.123456789,32345678.123456789012345678901234567890);

insert into PGC_ORA2PG_TYPES (ID,COL_NUM_4,COL_NUM_9,COL_NUM_18,COL_NUM_38,COL_NUM,COL_NUM_10_2,COL_NUM_FLOAT,COL_FLOAT32,COL_FLOAT64,COL_VARCHAR_30,COL_CHAR_2,COL_NVARCHAR_30,COL_NCHAR_2,COL_DATE,COL_TS,COL_TSTZ,COL_RAW,COL_LONG_RAW,COL_BLOB,COL_CLOB,COL_NCLOB) values
	 (1,1111,123456789,123456789012345678,1234567890123456789012345,123.1,123.10,123.123,123456.1,12345678.1,'Hello DVT','A ','Hello DVT','A ','1970-01-01','1970-01-01 00:00:01.123456','1969-12-31 19:00:01.123456-05',NULL,NULL,NULL,'DVT A','DVT A'),
	 (2,2222,123456789,123456789012345678,1234567890123456789012345,123.12,123.11,123.123,123456.1,12345678.1,'Hello DVT','B ','Hello DVT','B ','1970-01-02','1970-01-02 00:00:01.123456','1970-01-01 21:00:02.123456-05',NULL,NULL,NULL,'DVT B','DVT B'),
	 (3,3333,123456789,123456789012345678,1234567890123456789012345,123.123,123.11,123.123,123456.1,12345678.1,'Hello DVT','C ','Hello DVT','C ','1970-01-03','1970-01-03 00:00:01.123456','1970-01-02 22:00:03.123456-05',NULL,NULL,NULL,'DVT C','DVT C');

insert into PGC_PG_TYPES (COL_INT2,COL_INT4,COL_INT8,COL_DEC,COL_DEC_10_2,COL_FLOAT32,COL_FLOAT64,COL_VARCHAR_30,COL_CHAR_2,COL_TEXT,COL_DATE,COL_TS,COL_TSTZ,COL_TIME,COL_TIMETZ,COL_BINARY,COL_BOOL,COL_UUID,COL_OID) values
	 (1111,123456789,123456789012345678,12345678901234567890.12345,123.12,123456.1,12345678.1,'Hello DVT','A ','Hello DVT','1970-01-01','1970-01-01 00:00:01.123456','1969-12-31 19:00:01.123456-05','00:00:01.123456','00:00:01.123456+00',NULL,false,'2044eba4-595c-44e0-90c5-902603d4ef5a',1),
	 (2222,223456789,223456789012345678,22345678901234567890.12345,223.12,223456.1,22345678.1,'Hello DVT','B ','Hello DVT','1970-01-02','1970-01-02 00:00:02.123456','1970-01-01 19:00:02.123456-05','00:00:02.123456','00:00:02.123456+00',NULL,false,'1b90fceb-e1e6-42bc-9177-6603203c3139',2);

insert into PGC_TEST_GENERATE_PARTITIONS (COURSE_ID,QUARTER_ID,RECD_TIMESTAMP,REGISTRATION_DATE,APPROVED,GRADE) values
	 ('ALG001',1234,'2023-08-26 16:00:00','1969-07-20',true,3.5),
	 ('ALG001',1234,'2023-08-26 16:00:00','1969-07-20',false,2.8),
	 ('ALG001',5678,'2023-08-26 16:00:00','2023-08-23',true,2.1),
	 ('ALG001',5678,'2023-08-26 16:00:00','2023-08-23',false,3.5),
	 ('ALG003',1234,'2023-08-27 15:00:00','1969-07-20',true,3.5),
	 ('ALG003',1234,'2023-08-27 15:00:00','1969-07-20',false,2.8),
	 ('ALG003',5678,'2023-08-27 15:00:00','2023-08-23',true,2.1),
	 ('ALG003',5678,'2023-08-27 15:00:00','2023-08-23',false,3.5),
	 ('ALG002',1234,'2023-08-26 16:00:00','1969-07-20',true,3.5),
	 ('ALG002',1234,'2023-08-26 16:00:00','1969-07-20',false,2.8),
	 ('ALG002',5678,'2023-08-26 16:00:00','2023-08-23',true,2.1),
	 ('ALG002',5678,'2023-08-26 16:00:00','2023-08-23',false,3.5),
	 ('ALG004',1234,'2023-08-27 15:00:00','1969-07-20',true,3.5),
	 ('ALG004',1234,'2023-08-27 15:00:00','1969-07-20',false,2.8),
	 ('ALG004',5678,'2023-08-27 15:00:00','2023-08-23',true,2.1),
	 ('ALG004',5678,'2023-08-27 15:00:00','2023-08-23',false,3.5),
	 ('St. John''s',1234,'2023-08-26 16:00:00','1969-07-20',true,3.5),
	 ('St. John''s',1234,'2023-08-26 16:00:00','1969-07-20',false,2.8),
	 ('St. John''s',5678,'2023-08-26 16:00:00','2023-08-23',true,2.1),
	 ('St. John''s',5678,'2023-08-26 16:00:00','2023-08-23',false,3.5),
	 ('St. Jude''s',1234,'2023-08-27 15:00:00','1969-07-20',true,3.5),
	 ('St. Jude''s',1234,'2023-08-27 15:00:00','1969-07-20',false,2.8),
	 ('St. Jude''s',5678,'2023-08-27 15:00:00','2023-08-23',true,2.1),
	 ('St. Jude''s',5678,'2023-08-27 15:00:00','2023-08-23',false,3.5),
	 ('St. Edward''s',1234,'2023-08-26 16:00:00','1969-07-20',true,3.5),
	 ('St. Edward''s',1234,'2023-08-26 16:00:00','1969-07-20',false,2.8),
	 ('St. Edward''s',5678,'2023-08-26 16:00:00','2023-08-23',true,2.1),
	 ('St. Edward''s',5678,'2023-08-26 16:00:00','2023-08-23',false,3.5),
	 ('St. Paul''s',1234,'2023-08-27 15:00:00','1969-07-20',true,3.5),
	 ('St. Paul''s',1234,'2023-08-27 15:00:00','1969-07-20',false,2.8),
	 ('St. Paul''s',5678,'2023-08-27 15:00:00','2023-08-23',true,2.1),
	 ('St. Paul''s',5678,'2023-08-27 15:00:00','2023-08-23',false,3.5);

