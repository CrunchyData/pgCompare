/*
 * Copyright 2012-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crunchydata.util;

public interface SQLConstantsRepo {
    //
    // Repository DDL SQL
    //
    String REPO_DDL_SCHEMA="CREATE SCHEMA IF NOT EXISTS  %s AUTHORIZATION %s";

    // DC_PROJECT
    String REPO_DDL_DC_PROJECT = """
            CREATE TABLE dc_project (
            	pid int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
            	project_name text DEFAULT 'default'::text NOT NULL,
            	project_config jsonb NULL,
            	CONSTRAINT dc_project_pk PRIMARY KEY (pid)
            )
            """;

    String REPO_DDL_DC_PROJECT_DATA = """
            INSERT INTO dc_project (project_name) VALUES ('default')
            """;

    // DC_RESULT
    String REPO_DDL_DC_RESULT = """
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
            )
            """;

    String REPO_DDL_DC_RESULT_IDX1 = """
            CREATE INDEX dc_result_idx1 ON dc_result USING btree (table_name, compare_start)
            """;

    // DC_SOURCE
    String REPO_DDL_DC_SOURCE = """
            CREATE TABLE dc_source (
                tid int8 NULL,
                table_name text NULL,
                batch_nbr int4 NULL,
                pk jsonb NULL,
                pk_hash varchar(100) NULL,
                column_hash varchar(100) NULL,
                compare_result bpchar(1) NULL,
                thread_nbr int4 NULL
            )
            """;

    // DC_TABLE
    String REPO_DDL_DC_TABLE = """
            CREATE TABLE dc_table (
            	pid int8 DEFAULT 1 NOT NULL,
            	tid int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
            	table_alias text NULL,
            	status varchar(10) DEFAULT 'disabled'::character varying NULL,
            	batch_nbr int4 DEFAULT 1 NULL,
            	parallel_degree int4 DEFAULT 1 NULL,
            	CONSTRAINT dc_table_pk PRIMARY KEY (tid)
            )
            """;

    String REPO_DDL_DC_TABLE_IDX1 = """
            CREATE INDEX dc_table_idx1 ON dc_table USING btree (table_alias)
            """;


    // DC_TABLE_COLUMN
    String REPO_DDL_DC_TABLE_COLUMN = """
            CREATE TABLE dc_table_column (
            	tid int8 NOT NULL,
            	column_id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
            	column_alias varchar(50) NOT NULL,
            	status varchar(15) DEFAULT 'compare'::character varying NULL,
            	CONSTRAINT dc_table_column_pk PRIMARY KEY (column_id)
            )
            """;

    String REPO_DDL_DC_TABLE_COLUMN_IDX1 = """
            CREATE INDEX dc_table_column_idx1 ON dc_table_column USING btree (column_alias, tid, column_id)
            """;

    String REPO_DDL_DC_TABLE_COLUMN_FK = """
            ALTER TABLE dc_table_column ADD CONSTRAINT dc_table_column_fk FOREIGN KEY (tid) REFERENCES dc_table(tid) ON DELETE CASCADE
            """;

    // DC_TABLE_COLUMN_MAP
    String REPO_DDL_DC_TABLE_COLUMN_MAP = """
            CREATE TABLE dc_table_column_map (
            	tid int8 NOT NULL,
            	column_id int8 NOT NULL,
            	column_origin varchar(10) DEFAULT 'source'::character varying NOT NULL,
            	column_name varchar(50) NOT NULL,
            	data_type text NOT NULL,
            	data_class varchar(20) DEFAULT 'string'::character varying NULL,
            	data_length int4 NULL,
            	number_precision int4 NULL,
            	number_scale int4 NULL,
            	column_nullable bool DEFAULT true NULL,
            	column_primarykey bool DEFAULT false NULL,
            	map_expression varchar(500) NULL,
            	supported bool DEFAULT true NULL,
            	preserve_case bool DEFAULT false NULL,
            	map_type varchar(15) DEFAULT 'column'::character varying NOT NULL,
            	CONSTRAINT dc_table_column_map_pk PRIMARY KEY (column_id, column_origin, column_name)
            )
            """;

    String REPO_DDL_DC_TABLE_COLUMN_MAP_FK = """
            ALTER TABLE dc_table_column_map ADD CONSTRAINT dc_table_column_map_fk FOREIGN KEY (column_id) REFERENCES dc_table_column(column_id) ON DELETE CASCADE
            """;


    // DC_TABLE_HISTORY
    String REPO_DDL_DC_TABLE_HISTORY = """
            CREATE TABLE dc_table_history (
            	tid int8 NOT NULL,
            	load_id varchar(100) NULL,
            	batch_nbr int4 NOT NULL,
            	start_dt timestamptz NOT NULL,
            	end_dt timestamptz NULL,
            	action_result jsonb NULL,
            	action_type varchar(20) NOT NULL,
            	row_count int8 NULL
            )
            """;

    String REPO_DDL_DC_TABLE_HISTORY_IDX1 = """
            CREATE INDEX dc_table_history_idx1 ON dc_table_history USING btree (tid, start_dt)
            """;

    // DC_TABLE_MAP
    String REPO_DDL_DC_TABLE_MAP = """
            CREATE TABLE dc_table_map (
            	tid int8 NOT NULL,
            	dest_type varchar(20) DEFAULT 'target'::character varying NOT NULL,
            	schema_name text NOT NULL,
            	table_name text NOT NULL,
            	parallel_degree int4 DEFAULT 1 NULL,
            	mod_column varchar(200) NULL,
            	table_filter varchar(200) NULL,
            	schema_preserve_case bool DEFAULT false NULL,
            	table_preserve_case bool DEFAULT false NULL,
            	CONSTRAINT dc_table_map_pk PRIMARY KEY (tid, dest_type, schema_name, table_name)
            )
            """;

    String REPO_DDL_DC_TABLE_MAP_FK = """
            ALTER TABLE dc_table_map ADD CONSTRAINT dc_table_map_fk FOREIGN KEY (tid) REFERENCES dc_table(tid) ON DELETE CASCADE
            """;

    // DC_TARGET
    String REPO_DDL_DC_TARGET = """
            CREATE TABLE dc_target (
            	tid int8 NULL,
            	table_name text NULL,
            	batch_nbr int4 NULL,
            	pk jsonb NULL,
            	pk_hash varchar(100) NULL,
            	column_hash varchar(100) NULL,
            	compare_result bpchar(1) NULL,
            	thread_nbr int4 NULL
            )
            """;


    //
    // Repository SQL - Compare Task SQL
    //
    String SQL_REPO_CLEARMATCH = """
                WITH ds AS (DELETE FROM dc_source s
                            WHERE EXISTS
                                      (SELECT 1
                                       FROM dc_target t
                                       WHERE s.tid = t.tid
                                             AND s.pk_hash = t.pk_hash
                                             AND s.column_hash = t.column_hash)
                            RETURNING tid, pk_hash, column_hash)
                DELETE FROM dc_target dt USING ds
                WHERE  ds.tid=dt.tid
                       AND ds.pk_hash=dt.pk_hash
                       AND ds.column_hash=dt.column_hash
                """;

    String SQL_REPO_DCSOURCE_MARKNOTEQUAL = """
                                 UPDATE dc_source s SET compare_result = 'n'
                                 WHERE s.tid=?
                                       AND EXISTS (SELECT 1 FROM dc_target t WHERE t.tid=? AND s.pk_hash=t.pk_hash AND s.column_hash != t.column_hash)
                                 """;

    String SQL_REPO_DCSOURCE_MARKMISSING = """
                                      UPDATE dc_target t SET compare_result = 'm'
                                      WHERE t.tid=?
                                            AND NOT EXISTS (SELECT 1 FROM dc_source s WHERE s.tid=? AND t.pk_hash=s.pk_hash)
                                      """;

    String SQL_REPO_DCTARGET_MARKMISSING = """
                                      UPDATE dc_source s SET compare_result = 'm'
                                      WHERE s.tid=?
                                            AND NOT EXISTS (SELECT 1 FROM dc_target t WHERE t.tid=? AND s.pk_hash=t.pk_hash)
                                      """;
    String SQL_REPO_DCTARGET_MARKNOTEQUAL ="""
                                UPDATE dc_target t SET compare_result = 'n'
                                WHERE t.tid=?
                                      AND EXISTS (SELECT 1 FROM dc_source s WHERE s.tid=? AND t.pk_hash=s.pk_hash AND t.column_hash != s.column_hash)
                                """;

    String SQL_REPO_SELECT_OUTOFSYNC_ROWS = """
                        SELECT DISTINCT tid, pk_hash, pk
                        FROM (SELECT tid, pk_hash, pk
                            FROM dc_source
                            WHERE tid = ?
                                  AND compare_result is not null
                                  AND compare_result != 'e'
                            UNION
                            SELECT tid, pk_hash, pk
                            FROM dc_target
                            WHERE tid = ?
                                  AND compare_result is not null
                                  AND compare_result != 'e') x
                        ORDER BY tid
                       """;


    //
    // Repository SQL - DC_RESULT
    //
    String SQL_REPO_DCRESULT_INSERT = "INSERT INTO dc_result (compare_start, tid, table_name, equal_cnt, missing_source_cnt, missing_target_cnt, not_equal_cnt, source_cnt, target_cnt, status, rid) values (current_timestamp, ?, ?, 0, 0, 0, 0, 0, 0, 'running', ?) returning cid";
    String SQL_REPO_DCRESULT_UPDATECNT = """
                                 UPDATE dc_result SET equal_cnt=equal_cnt+?
                                 WHERE cid=?
                                 """;

    String SQL_REPO_DCRESULT_UPDATE_ALLCOUNTS = """
                                 UPDATE dc_result SET equal_cnt=equal_cnt+?, source_cnt=source_cnt+?, target_cnt=target_cnt+?
                                 WHERE cid=?
                                 """;

    String SQL_REPO_DCRESULT_UPDATE_STATUSANDCOUNT = """
                                 UPDATE dc_result SET missing_source_cnt=?, missing_target_cnt=?, not_equal_cnt=?, status=?, compare_end=current_timestamp
                                 WHERE cid=?
                                 RETURNING equal_cnt, missing_source_cnt, missing_target_cnt, not_equal_cnt, status
                                 """;

    String SQL_REPO_DCRESULT_CLEAN = """
            DELETE FROM dc_result WHERE tid NOT IN (SELECT tid FROM dc_table)
            """;

    String SQL_REPO_DCRESULT_REPORT = """
            SELECT table_name as "Table", status as "Status", compare_start as "Compare Start", compare_end as "Compare End",
                   EXTRACT(EPOCH FROM (compare_end-compare_start)) as "Run Time (s)",
                   source_cnt as "Source Count", target_cnt as "Target Count", 
                   equal_cnt as "Equal", not_equal_cnt as "Not Equal", 
                   missing_source_cnt as "Missing Source", missing_target_cnt as "Missing Target"
            FROM dc_result
            WHERE rid=?
            ORDER BY compare_start
            """;

    //
    // Repository SQL - DC_SOURCE
    //
    String SQL_REPO_DCSOURCE_DELETEBYTIDBATCHNBR = "DELETE FROM dc_source WHERE tid=? AND batch_nbr=?";

    String SQL_REPO_DCSOURCE_DELETE = "DELETE FROM dc_source WHERE tid=? AND pk_hash=? AND batch_nbr=?";
    String SQL_REPO_DCSOURCE_INSERT = """
                INSERT INTO dc_source (tid, thread_nbr, pk_hash, column_hash, pk, compare_result, batch_nbr, table_name) (SELECT ? tid, ? thread_nbr, pk_hash, column_hash, pk, compare_result, ? batch_nbr, ? table_alias FROM stagingtable)
                """;

    String SQL_REPO_DCSOURCE_CLEAN = """
            DELETE FROM dc_source WHERE tid NOT IN (SELECT tid FROM dc_table)
            """;
    //
    // Repository SQL - DC_TARGET
    //
    String SQL_REPO_DCTARGET_DELETEBYTIDBATCHNBR = "DELETE FROM dc_target WHERE tid=? AND batch_nbr=?";

    String SQL_REPO_DCTARGET_DELETE = "DELETE FROM dc_target WHERE tid=? AND pk_hash=? AND batch_nbr=?";

    String SQL_REPO_DCTARGET_CLEAN = """
            DELETE FROM dc_target WHERE tid NOT IN (SELECT tid FROM dc_table)
            """;


    //
    // Repository SQL - DC_TABLE
    //
    String SQL_REPO_DCTABLE_DELETEBYPROJECT = "DELETE FROM dc_table WHERE pid=?";

    String SQL_REPO_DCTABLE_DELETEBYPROJECTTABLE = "DELETE FROM dc_table WHERE pid=? AND table_alias=?";

    String SQL_REPO_DCTABLE_DELETEBYTID = "DELETE FROM dc_table WHERE tid=?";

    String SQL_REPO_DCTABLE_INCOMPLETEMAP = """
                SELECT t.tid, t.table_alias, count(1) cnt
                FROM dc_table t
                     LEFT OUTER JOIN dc_table_map m ON (t.tid = m.tid)
                WHERE t.pid = ?
                GROUP BY t.tid
                HAVING count(1) < 2
                """;
    String SQL_REPO_DCTABLE_INSERT = "INSERT INTO dc_table (pid, table_alias, batch_nbr, status) VALUES (?, lower(?), 1, 'enabled') RETURNING tid";

    String SQL_REPO_DCTABLE_SELECTBYPID = """
                     SELECT pid, tid, table_alias, status, batch_nbr, parallel_degree
                     FROM dc_table
                     WHERE pid=?
                     """;

    String SQL_REPO_DCTABLE_SELECT_BYNAME = "SELECT tid FROM dc_table WHERE table_alias = lower(?) AND pid=?";


    //
    // Repository SQL - DC_TABLE_COLUMN
    //

    String SQL_REPO_DCTABLECOLUMN_INSERT = "INSERT INTO dc_table_column (tid, column_alias) VALUES (?, ?) RETURNING column_id";

    String SQL_REPO_DCTABLECOLUMN_SELECTBYTIDALIAS = "SELECT column_id FROM dc_table_column WHERE tid=? AND column_alias=lower(?)";

    String SQL_REPO_DCTABLECOLUMN_DELETEBYCOLUMNID = "DELETE FROM dc_table_column WHERE column_id=?";

    String SQL_REPO_DCTABLECOLUMN_DELETEBYPID = "DELETE FROM dc_table_column WHERE tid IN (SELECT tid FROM dc_table WHERE pid=?)";

    String SQL_REPO_DCTABLECOLUMN_DELETEBYPIDTABLE = "DELETE FROM dc_table_column WHERE tid IN (SELECT tid FROM dc_table WHERE pid=? AND table_alias=?)";

    String SQL_REPO_DCTABLECOLUMN_DELETEBYTID = "DELETE FROM dc_table_column WHERE tid=?";

    String SQL_REPO_DCTABLECOLUMN_INCOMPLETEMAP = """
                SELECT t.tid, t.column_id, t.column_alias, count(1) cnt
                FROM dc_table_column t
                     LEFT OUTER JOIN dc_table_column_map m ON (t.column_id = m.column_id)
                WHERE t.tid = ?
                GROUP BY t.tid, t.column_id, t.column_alias
                HAVING count(1) < 2
                """;

    //
    // Repository SQL - DC_TABLE_COLUMN_MAP
    //
    String SQL_REPO_DCTABLECOLUMNMAP_FULLBYTID = """
            WITH column_map AS (
            	    SELECT
            	        tcm.tid,
            	        tcm.column_id,
            	        tcm.column_origin,
            	        jsonb_build_object(
            	            'columnName', tcm.column_name,
            	            'dataType', tcm.data_type,
            	            'dataClass', tcm.data_class,
            	            'dataLength', tcm.data_length,
            	            'numberPrecision', tcm.number_precision,
            	            'numberScale', tcm.number_scale,
            	            'nullable', tcm.column_nullable,
            	            'primaryKey', tcm.column_primarykey,
            	            'valueExpression', tcm.map_expression,
            	            'supported', tcm.supported,
            	            'preserveCase', tcm.preserve_case,
            	            'mapType', tcm.map_type
            	        ) AS columnInfo
            	    FROM dc_table_column_map tcm
            	),
            	tcmt AS (
            	    SELECT tid, column_id, columnInfo
            	    FROM column_map
            	    WHERE column_origin = 'target'
            	),
            	tcms AS (
            	    SELECT tid, column_id, columnInfo
            	    FROM column_map
            	    WHERE column_origin = 'source'
            	)
            SELECT
                jsonb_build_object(
                    'tid', t.tid,
                    'tableAlias', t.table_alias,
                    'columns', jsonb_agg(
                        jsonb_build_object(
                            'columnID', tc.column_id,
                            'columnAlias', tc.column_alias,
                            'source', tcms.columnInfo,
                            'target', tcmt.columnInfo
                        )
                    )
                )
            FROM
                dc_table t
                JOIN dc_table_column tc ON t.tid = tc.tid
                JOIN tcmt ON tc.tid = tcmt.tid AND tc.column_id = tcmt.column_id
                JOIN tcms ON tc.tid = tcms.tid AND tc.column_id = tcms.column_id
            WHERE
                t.tid = ?
            group by t.tid, t.table_alias
            """;

    String SQL_REPO_DCTABLECOLUMNMAP_BYORIGINALIAS = """
            SELECT c.tid, c.column_id, m.column_name, m.preserve_case
            FROM dc_table_column c
                 JOIN dc_table_column_map m ON (c.tid = m.tid AND c.column_id = m.column_id)
            WHERE c.tid = ?
                  AND c.column_alias = ?
                  AND m.column_origin = ?;
            """;


    String SQL_REPO_DCTABLECOLUMNMAP_INSERT = "INSERT INTO dc_table_column_map (tid, column_id, column_origin, column_name, data_type, data_class, data_length, number_precision, number_scale, column_nullable, column_primarykey, map_expression, supported, preserve_case) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    //
    // Repository SQL - DC_TABLE_HISTORY
    //
    String SQL_REPO_DCTABLEHISTORY_INSERT = "INSERT INTO dc_table_history (tid, action_type, start_dt, load_id, batch_nbr, row_count) VALUES (?, ?, current_timestamp, ?, ?, 0)";

    String SQL_REPO_DCTABLEHISTORY_UPDATE = "UPDATE dc_table_history set end_dt=current_timestamp, row_count=?, action_result=?::jsonb WHERE tid=? AND action_type=? and load_id=? and batch_nbr=?";

    //
    // Repository SQL - DC_TABLE_MAP
    //
    String SQL_REPO_DCTABLEMAP_SELECTBYTIDORIGIN = "SELECT tid, dest_type, schema_name, table_name, parallel_degree, mod_column, table_filter, schema_preserve_case, table_preserve_case FROM dc_table_map WHERE tid=? and dest_type=?";
    String SQL_REPO_DCTABLEMAP_INSERT = "INSERT INTO dc_table_map (tid, dest_type, schema_name, table_name, schema_preserve_case, table_preserve_case) VALUES (?, ?, ?, ?, ?, ?)";

    String SQL_REPO_DCTABLEMAP_SELECTBYPIDORIGIN = """
            SELECT t.tid, t.table_alias, m.schema_name, m.table_name
            FROM dc_table t
                 JOIN dc_table_map m ON (t.tid=m.tid)
            WHERE t.pid = ?
                  AND m.dest_type = ?
    """;

    String SQL_REPO_DCTABLEMAP_SELECTBYPIDORIGINTABLE = """
            SELECT t.tid, t.table_alias, m.schema_name, m.table_name
            FROM dc_table t
                 JOIN dc_table_map m ON (t.tid=m.tid)
            WHERE t.pid = ?
                  AND m.dest_type = ?
                  AND t.table_alias = ?
    """;
}
