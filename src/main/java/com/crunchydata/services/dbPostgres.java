/*
 * Copyright 2012-2023 the original author or authors.
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
package com.crunchydata.services;

import com.crunchydata.util.Logging;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author Brian Pace
 */
public class dbPostgres {

    public static String buildLoadSQL (Boolean useDatabaseHash, String schema, String tableName, String pkColumns, String pkJSON, String columns, String tableFilter) {
        String sql = "SELECT ";

        if (useDatabaseHash) {
            sql += "md5(concat_ws('|'," + pkColumns + ")) pk_hash, " + pkJSON + " pk, md5(concat_ws(''," + columns + ")) FROM " + schema + "." + tableName + " WHERE 1=1 ";
        } else {
            sql += pkColumns + " pk_hash, " + pkJSON + " pk, " + columns + " FROM " + schema + "." + tableName + " WHERE 1=1 ";
        }

        if (tableFilter != null && !tableFilter.isEmpty()) {
            sql += " AND " + tableFilter;
        }

        return sql;
    }
    public static CachedRowSet getColumns (Connection conn, String schema, String table) {
        ResultSet rs;
        PreparedStatement stmt;
        CachedRowSet crs = null;

        String sql = """
                SELECT DISTINCT n.nspname owner, t.relname table_name, c.attname column_name,
                        col.udt_name data_type, coalesce(col.character_maximum_length,col.numeric_precision) data_length,
                                col.numeric_precision data_precision, col.numeric_scale data_scale,
                        CASE WHEN c.attnotnull THEN 'Y' ELSE 'N' END nullable,
                        CASE WHEN i.indisprimary THEN 'Y' ELSE 'N' END pk
                FROM pg_class t
                     JOIN pg_attribute c ON (t.oid=c.attrelid)
                     JOIN pg_namespace n ON (t.relnamespace=n.oid)
                     JOIN information_schema.columns col ON (col.table_schema=n.nspname AND col.table_name=t.relname AND col.column_name=c.attname)
                     LEFT OUTER JOIN pg_index i ON (i.indrelid=c.attrelid AND c.attnum = any(i.indkey) AND i.indisunique)
                WHERE n.nspname=lower(?)
                      AND t.relname=lower(?)
                ORDER BY n.nspname, t.relname, c.attname
                """;
        try {
            crs = RowSetProvider.newFactory().createCachedRowSet();
            stmt = conn.prepareStatement(sql);
            stmt.setObject(1, schema);
            stmt.setObject(2,table);
            rs = stmt.executeQuery();
            crs.populate(rs);
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", "postgres-service", "Error retrieving columns for table " + schema + "." + table + ":  " + e.getMessage());
        }
        return crs;

    }

    public static Connection getConnection(Properties connectionProperties, String destType, String module) {
        Connection conn;
        conn = null;

        String url = "jdbc:postgresql://"+connectionProperties.getProperty(destType+"-host")+":"+connectionProperties.getProperty(destType+"-port")+"/"+connectionProperties.getProperty(destType+"-dbname");
        Properties dbProps = new Properties();
        dbProps.setProperty("user",connectionProperties.getProperty(destType+"-user"));
        dbProps.setProperty("password",connectionProperties.getProperty(destType+"-password"));
        dbProps.setProperty("options","-c search_path="+connectionProperties.getProperty(destType+"-schema")+",public,pg_catalog");
        dbProps.setProperty("reWriteBatchedInserts", "true");
        dbProps.setProperty("preparedStatementCacheQueries", "5");
        dbProps.setProperty("ApplicationName", "ConferoDC - " + module);

        try {
            conn = DriverManager.getConnection(url,dbProps);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            Logging.write("severe", "postgres-service", "Error connecting to Postgres: " + e.getMessage());
        }

        return conn;

    }

    public static String getVersion (Connection conn) {
        String dbVersion = null;

        ArrayList<Object> binds = new ArrayList<>();

        try {
            CachedRowSet crsVersion = dbPostgres.simpleSelect(conn, "select v.ver[2]::numeric version from (select string_to_array(version(),' ') as ver) v", binds);

            if (crsVersion.next()) {
                dbVersion = crsVersion.getString("version");
            }

            crsVersion.close();

        } catch (Exception e) {
            Logging.write("warning", "postgres-service", "Could not retrieve Postgres version: " + e.getMessage());
        }

        return dbVersion;
    }

    public static CachedRowSet simpleSelect(Connection conn, String sql, ArrayList<Object> binds) {
        ResultSet rs;
        PreparedStatement stmt;
        CachedRowSet crs = null;

        try {
            crs = RowSetProvider.newFactory().createCachedRowSet();
            stmt = conn.prepareStatement(sql);
            for (int counter = 0; counter < binds.size(); counter++) {
                stmt.setObject(counter+1, binds.get(counter));
            }
            rs = stmt.executeQuery();
            crs.populate(rs);
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", "postgres-service", "Error executing simple select (" + sql + "):  " + e.getMessage());
        }
        return crs;
    }

    public static Integer simpleUpdate(Connection conn, String sql, ArrayList<Object> binds, Boolean commit) {
        int cnt;
        PreparedStatement stmt;

        try {
            stmt = conn.prepareStatement(sql);
            stmt.setFetchSize(2000);
            for (int counter = 0; counter < binds.size(); counter++) {
                stmt.setObject(counter+1, binds.get(counter));
            }
            cnt = stmt.executeUpdate();
            stmt.close();
            if (commit) {
                conn.commit();
            }
        } catch (Exception e) {
            Logging.write("severe", "postgres-service", "Error executing simple update (" + sql + "):  " + e.getMessage());
            try { conn.rollback(); } catch (Exception ee) {
                // do nothing
            }
            cnt = -1;
        }
        return cnt;
    }

    public static CachedRowSet simpleUpdateReturning(Connection conn, String sql, ArrayList<Object> binds) {
        PreparedStatement stmt;
        CachedRowSet crs = null;

        try {
            crs = RowSetProvider.newFactory().createCachedRowSet();
            stmt = conn.prepareStatement(sql);
            stmt.setFetchSize(2000);
            for (int counter = 0; counter < binds.size(); counter++) {
                stmt.setObject(counter+1, binds.get(counter));
            }
            ResultSet rs = stmt.executeQuery();
            crs.populate(rs);
            rs.close();
            stmt.close();
            conn.commit();
        } catch (Exception e) {
            Logging.write("severe", "postgres-service", "Error executing simple update with returning (" + sql + "):  " + e.getMessage());
            try { conn.rollback(); } catch (Exception ee) {
                // do nothing
            }
        }

        return crs;
    }


    public static void simpleExecute(Connection conn, String sql) {
        PreparedStatement stmt;

        try {
            stmt = conn.prepareStatement(sql);
            stmt.execute();
            stmt.close();
            conn.commit();
        } catch (Exception e) {
            Logging.write("severe", "postgres-service", "Error executing simple execute (" + sql + "):  " + e.getMessage());
            try { conn.rollback(); } catch (Exception ee) {
                // do nothing
            }
        }
    }
}
