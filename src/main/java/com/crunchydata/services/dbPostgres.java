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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

import com.crunchydata.util.Logging;

import org.json.JSONArray;
import org.json.JSONObject;

import static com.crunchydata.services.ColumnValidation.*;
import static com.crunchydata.util.Settings.Props;

/**
 * @author Brian Pace
 */
public class dbPostgres {

    public static String buildLoadSQL (Boolean useDatabaseHash, String schema, String tableName, String pkColumns, String pkJSON, String columns, String tableFilter) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
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

    public static String columnValueMapPostgres(JSONObject column) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        String colExpression;

        if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = switch (column.getString("dataType").toLowerCase()) {
                case "float4", "float8" ->
                        "coalesce(trim(to_char(" + column.getString("columnName") + ",'0.999999EEEE')),' ')";
                default ->
                        Props.getProperty("number-cast").equals("notation") ? "coalesce(trim(to_char(" + column.getString("columnName") + ",'0.9999999999EEEE')),' ')" : "coalesce(trim(to_char(trim_scale(" + column.getString("columnName") + "),'0000000000000000000000.0000000000000000000000')),' ')";
            };

        } else if ( Arrays.asList(booleanTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "case when coalesce(" + column.getString("columnName") + "::text,'0') = 'true' then '1' else '0' end";
        } else if ( Arrays.asList(timestampTypes).contains(column.getString("dataType").toLowerCase()) ) {
            if (column.getString("dataType").toLowerCase().contains("time zone") || column.getString("dataType").toLowerCase().contains("tz") ) {
                colExpression = "coalesce(to_char(" + column.getString("columnName") + " at time zone 'UTC','MMDDYYYYHH24MISS'),' ')";
            } else {
                colExpression = "coalesce(to_char(" + column.getString("columnName") + ",'MMDDYYYYHH24MISS'),' ')";
            }
        } else if ( Arrays.asList(charTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "coalesce(" + column.getString("columnName") + "::text,' ')";
        } else if ( Arrays.asList(binaryTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "coalesce(md5(" + column.getString("columnName") +"), ' ')";
        } else {
            colExpression = column.getString("columnName");
        }

        return colExpression;

    }

    public static JSONArray getColumns (Connection conn, String schema, String table) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        ResultSet rs;
        PreparedStatement stmt;
        JSONArray columnInfo = new JSONArray();

        /////////////////////////////////////////////////
        // SQL
        /////////////////////////////////////////////////
        String sql = """
                SELECT DISTINCT n.nspname as owner, t.relname table_name, c.attname column_name,
                        col.udt_name data_type, coalesce(col.character_maximum_length,col.numeric_precision) data_length,
                                coalesce(col.numeric_precision,44) data_precision, coalesce(col.numeric_scale,22) data_scale,
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
            stmt = conn.prepareStatement(sql);
            stmt.setObject(1, schema);
            stmt.setObject(2,table);
            rs = stmt.executeQuery();
            while (rs.next()) {
                JSONObject column = new JSONObject();
                if (Arrays.asList(unsupportedDataTypes).contains(rs.getString("data_type").toLowerCase()) ) {
                    Logging.write("severe", "postgres-service", "Unsupported data type (" + rs.getString("data_type") + ")");
                    //System.exit(1);
                    column.put("supported",false);
                } else {
                    column.put("supported",true);
                }
                column.put("columnName",rs.getString("column_name"));
                column.put("dataType",rs.getString("data_type"));
                column.put("dataLength",rs.getInt("data_length"));
                column.put("dataPrecision",rs.getInt("data_precision"));
                column.put("dataScale",rs.getInt("data_scale"));
                column.put("nullable",rs.getString("nullable").equals("Y"));
                column.put("primaryKey",rs.getString("pk").equals("Y"));
                column.put("valueExpression", columnValueMapPostgres(column ));
                column.put("dataClass", getDataClass(rs.getString("data_type").toLowerCase()));

                columnInfo.put(column);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", "postgres-service", "Error retrieving columns for table " + schema + "." + table + ":  " + e.getMessage());
        }
        return columnInfo;
    }

    public static Connection getConnection(Properties connectionProperties, String destType, String module) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        Connection conn;
        conn = null;
        String url = "jdbc:postgresql://"+connectionProperties.getProperty(destType+"-host")+":"+connectionProperties.getProperty(destType+"-port")+"/"+connectionProperties.getProperty(destType+"-dbname")+"?sslmode="+connectionProperties.getProperty(destType+"-sslmode");
        Properties dbProps = new Properties();

        dbProps.setProperty("user",connectionProperties.getProperty(destType+"-user"));
        dbProps.setProperty("password",connectionProperties.getProperty(destType+"-password"));
        dbProps.setProperty("options","-c search_path="+connectionProperties.getProperty(destType+"-schema")+",public,pg_catalog");
        dbProps.setProperty("reWriteBatchedInserts", "true");
        dbProps.setProperty("preparedStatementCacheQueries", "5");
        dbProps.setProperty("ApplicationName", "pgCompare - " + module);

        try {
            conn = DriverManager.getConnection(url,dbProps);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            Logging.write("severe", "postgres-service", "Error connecting to Postgres: " + e.getMessage());
        }

        return conn;

    }

    public static JSONArray getTables (Connection conn, String schema) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        ResultSet rs;
        PreparedStatement stmt;
        JSONArray tableInfo = new JSONArray();

        /////////////////////////////////////////////////
        // SQL
        /////////////////////////////////////////////////
        String sql = """
                SELECT lower(table_schema) owner, lower(table_name) table_name
                FROM  information_schema.tables
                WHERE table_schema=lower(?)  
                      AND table_type != 'VIEW'                    
                ORDER BY table_schema, table_name
                """;
        try {
            stmt = conn.prepareStatement(sql);
            stmt.setObject(1, schema);
            rs = stmt.executeQuery();

            while (rs.next()) {
                JSONObject table = new JSONObject();
                table.put("schemaName",rs.getString("owner"));
                table.put("tableName",rs.getString("table_name"));

                tableInfo.put(table);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", "postgres-service", "Error retrieving tables for " + schema + ":  " + e.getMessage());
        }
        return tableInfo;
    }

    public static String getVersion (Connection conn) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
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
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
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
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
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
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
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
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
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
