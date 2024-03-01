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


public class dbOracle {

    public static String buildLoadSQL (Boolean useDatabaseHash, String schema, String tableName, String pkColumns, String pkJSON, String columns, String tableFilter) {
        String sql = "SELECT ";

        if (useDatabaseHash) {
            sql += "LOWER(STANDARD_HASH(" + pkColumns + ",'MD5')) pk_hash, " + pkJSON + " pk, LOWER(STANDARD_HASH(" + columns + ",'MD5')) column_hash FROM " + schema + "." + tableName + " WHERE 1=1 ";
        } else {
            sql += pkColumns + " pk_hash, " + pkJSON + " pk, " + columns + " FROM " + schema + "." + tableName + " WHERE 1=1 ";
        }

        if ( tableFilter != null && !tableFilter.isEmpty()) {
            sql += " AND " + tableFilter;
        }

        return sql;
    }

    public static String columnValueMapOracle(JSONObject column) {
        String colExpression;

        if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) ) {

            colExpression = switch (column.getString("dataType").toLowerCase()) {
                case "float", "binary_float", "binary_double" ->
                        "lower(nvl(trim(to_char(" + column.getString("columnName") + ",'0.999999EEEE')),' '))";
                default ->
                        Props.getProperty("number-cast").equals("notation") ? "lower(nvl(trim(to_char(" + column.getString("columnName") + ",'0.9999999999EEEE')),' '))" : "nvl(trim(to_char(" + column.getString("columnName") + ",'0000000000000000000000.0000000000000000000000')),' ')";
            };


        } else if ( Arrays.asList(booleanTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "nvl(to_char(" + column.getString("columnName") + "),'0')";
        } else if ( Arrays.asList(timestampTypes).contains(column.getString("dataType").toLowerCase()) ) {
            if (column.getString("dataType").toLowerCase().contains("time zone") || column.getString("dataType").toLowerCase().contains("tz") ) {
                colExpression = "nvl(to_char(" + column.getString("columnName") + " at time zone 'UTC','MMDDYYYYHH24MISS'),' ')";
            } else {
                colExpression = "nvl(to_char(" + column.getString("columnName") + ",'MMDDYYYYHH24MISS'),' ')";
            }
        } else if ( Arrays.asList(charTypes).contains(column.getString("dataType").toLowerCase()) ) {
            if (column.getString("dataType").toLowerCase().contains("lob")) {
                colExpression = "nvl(trim(to_char(" + column.getString("columnName") + ")),' ')";
            } else {
                if (column.getInt("dataLength") > 1) {
                    colExpression = "nvl(trim(" + column.getString("columnName") + "),' ')";
                } else {
                    colExpression = "nvl(" + column.getString("columnName") + ",' ')";
                }
            }
        } else if ( Arrays.asList(binaryTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "case when dbms_lob.getlength(" + column.getString("columnName") +") = 0 or " + column.getString("columnName") + " is null then ' ' else lower(dbms_crypto.hash(" + column.getString("columnName") + ",2)) end";
        } else {
            colExpression = column.getString("columnName");
        }

        return colExpression;

    }

    public static JSONArray getColumns (Connection conn, String schema, String table) {
        ResultSet rs;
        PreparedStatement stmt;
        JSONArray columnInfo = new JSONArray();

        String sql = """
                SELECT LOWER(c.owner) owner, LOWER(c.table_name) table_name, LOWER(c.column_name) column_name, LOWER(c.data_type) data_type, c.data_length, nvl(c.data_precision,44) data_precision, nvl(c.data_scale,22) data_scale, c.nullable,
                       CASE WHEN pkc.column_name IS NULL THEN 'N' ELSE 'Y' END pk
                FROM all_tab_columns c
                     LEFT OUTER JOIN (SELECT con.owner, con.table_name, i.column_name, i.column_position
                                    FROM all_constraints con
                                         JOIN all_ind_columns i ON (con.index_owner=i.index_owner AND con.index_name=i.index_name)
                                    WHERE con.constraint_type='P') pkc ON (c.owner=pkc.owner AND c.table_name=pkc.table_name AND c.column_name=pkc.column_name)
                WHERE c.owner=upper(?)
                      AND c.table_name=upper(?)
                ORDER BY c.owner, c.table_name, c.column_name
                """;

        try {
            stmt = conn.prepareStatement(sql);
            stmt.setObject(1, schema);
            stmt.setObject(2,table);
            rs = stmt.executeQuery();
            while (rs.next()) {
                JSONObject column = new JSONObject();
                if (Arrays.asList(unsupportedDataTypes).contains(rs.getString("data_type").toLowerCase()) ) {
                    Logging.write("warning", "oracle-service", "Unsupported data type (" + rs.getString("data_type") + ") for column " + rs.getString("column_name"));
                    column.put("supported",false);
                } else {
                    column.put("supported",true);
                }
                column.put("columnName",rs.getString("column_name"));
                column.put("dataType",rs.getString("data_type"));
                column.put("dataLength",rs.getInt("data_length"));
                column.put("dataPrecision",rs.getInt("data_precision"));
                column.put("dataScale",rs.getInt("data_scale"));
                column.put("nullable", rs.getString("nullable").equals("Y"));
                column.put("primaryKey",rs.getString("pk").equals("Y"));
                column.put("valueExpression", columnValueMapOracle(column));
                column.put("dataClass", getDataClass(rs.getString("data_type").toLowerCase()));

                columnInfo.put(column);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", "oracle-service", "Error retrieving columns for table " + schema + "." + table + ":  " + e.getMessage());
        }
        return columnInfo;
    }

    public static Connection getConnection(Properties connectionProperties, String destType) {
        Connection conn;
        conn = null;

        String url = "jdbc:oracle:thin:@//"+connectionProperties.getProperty(destType+"-host")+":"+connectionProperties.getProperty(destType+"-port")+"/"+connectionProperties.getProperty(destType+"-dbname");
        Properties dbProps = new Properties();
        dbProps.setProperty("user",connectionProperties.getProperty(destType+"-user"));
        dbProps.setProperty("password",connectionProperties.getProperty(destType+"-password"));

        try {
            conn = DriverManager.getConnection(url,dbProps);
        } catch (Exception e) {
            Logging.write("severe", "oracle-service", "Error connecting to Oracle " + e.getMessage());
        }

        return conn;

    }

    public static String getVersion (Connection conn) {
        String dbVersion = null;

        ArrayList<Object> binds = new ArrayList<>();

        try {
            CachedRowSet crsVersion = dbPostgres.simpleSelect(conn, "select version from v$version", binds);

            if (crsVersion.next()) {
                dbVersion = crsVersion.getString("version");
            }

            crsVersion.close();

        } catch (Exception e) {
            Logging.write("info", "oracle-service", "Could not retrieve Oracle version " + e.getMessage());
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
            stmt.setFetchSize(2000);
            for (int counter = 0; counter < binds.size(); counter++) {
                stmt.setObject(counter+1, binds.get(counter));
            }
            rs = stmt.executeQuery();
            crs.populate(rs);
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", "oracle-service", "Error executing simple select (" + sql + "): " + e.getMessage());
        }
        return crs;
    }

    public static Integer simpleUpdate(Connection conn, String sql, ArrayList<Object> binds, boolean commit) {
        int cnt;
        PreparedStatement stmt;

        try {
            stmt = conn.prepareStatement(sql);
            for (int counter = 0; counter < binds.size(); counter++) {
                stmt.setObject(counter+1, binds.get(counter));
            }
            cnt = stmt.executeUpdate();
            stmt.close();
            if (commit) {
                conn.commit();
            }
        } catch (Exception e) {
            Logging.write("severe", "oracle-service", "Error executing simple update (" + sql + "):  " + e.getMessage());
            try { conn.rollback(); } catch (Exception ee) {
                // do nothing
            }
            cnt = -1;
        }
        return cnt;
    }

}
