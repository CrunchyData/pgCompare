package com.crunchydata.services;

import java.sql.Connection;
import java.util.ArrayList;

public class dbRepository {
    /////////////////////////////////////////////////
    // SQL
    /////////////////////////////////////////////////
    static String sqlSchema = """
                       CREATE SCHEMA confero AUTHORIZATION postgres
                       """;

    static String sqlDCResult = """
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
                                CONSTRAINT dc_result_pk PRIMARY KEY (cid))
                         """;

    static String sqlDCResultIdx1 = """
                             CREATE INDEX dc_result_idx1 ON dc_result(table_name, compare_dt)
                             """;

    static String sqlDCSource = """
                         CREATE TABLE dc_source (
                                table_name text NULL,
                                batch_nbr int4 NULL,
                                pk jsonb NULL,
                                pk_hash varchar(100) NULL,
                                column_hash varchar(100) NULL,
                                compare_result bpchar(1) NULL,
                                thread_nbr int4 NULL)                         
                         """;

    static String sqlDCTarget = """
                        CREATE TABLE dc_target (
                                table_name text NULL,
                                batch_nbr int4 NULL,
                                pk jsonb NULL,
                                pk_hash varchar(100) NULL,
                                column_hash varchar(100) NULL,
                                compare_result bpchar(1) NULL,
                                thread_nbr int4 NULL)            
                         """;

    static String sqlDCTable = """
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
                                column_map jsonb)                        
                        """;

    static String sqlDCTablePK = """
                          ALTER TABLE dc_table ADD CONSTRAINT dc_table_pk PRIMARY KEY (tid)                      
                          """;

    static String sqlDCTableHistory = """
                                CREATE TABLE dc_table_history (
                                        tid int8 NOT NULL,
                                        load_id varchar(100) NULL,
                                        batch_nbr int4 NOT NULL,
                                        start_dt timestamptz NOT NULL,
                                        end_dt timestamptz NULL,
                                        action_result jsonb NULL,
                                        action_type varchar(20) NOT NULL,                                                                                
                                        row_count int8 NULL)                               
                               """;

    static String sqlDCTableHistoryIdx1 = """
                               CREATE INDEX dc_table_history_idx1 ON dc_table_history(tid, start_dt)
                               """;

    static String sqlDCObject = """
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
                                )                       
                                """;

    static String sqlDCObjectPK = """
                                  ALTER TABLE dc_object ADD CONSTRAINT dc_object_pk PRIMARY KEY (oid)
                                  """;


    public static void createRepository(Connection conn) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        ArrayList<Object> binds = new ArrayList<>();

        // Create Schema
        dbPostgres.simpleUpdate(conn,sqlSchema,binds, true);

        // Create Tables
        dbPostgres.simpleUpdate(conn,sqlDCResult, binds, true);
        dbPostgres.simpleUpdate(conn,sqlDCSource, binds, true);
        dbPostgres.simpleUpdate(conn,sqlDCTarget, binds, true);
        dbPostgres.simpleUpdate(conn,sqlDCTable, binds, true);
        dbPostgres.simpleUpdate(conn,sqlDCTableHistory, binds, true);
        dbPostgres.simpleUpdate(conn,sqlDCObject, binds, true);

        // Create Indexes
        dbPostgres.simpleUpdate(conn,sqlDCResultIdx1, binds, true);
        dbPostgres.simpleUpdate(conn,sqlDCTableHistoryIdx1, binds, true);

        // Add Constraints
        dbPostgres.simpleUpdate(conn,sqlDCTablePK, binds, true);
        dbPostgres.simpleUpdate(conn,sqlDCObjectPK, binds, true);

    }
}
