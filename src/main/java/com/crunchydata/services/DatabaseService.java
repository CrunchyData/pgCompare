package com.crunchydata.services;

import com.crunchydata.models.ColumnMetadata;
import com.crunchydata.models.DCTableMap;
import com.crunchydata.util.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import static com.crunchydata.util.DataUtility.ShouldQuoteString;
import static com.crunchydata.util.Settings.Props;

public class DatabaseService {

    private static final String THREAD_NAME = "database-service";

    public static String getNativeCase(String platform) {
        return switch (platform) {
            case "db2", "oracle" -> "upper";
            // mariadb, mssql, mysql, postgres
            default -> "lower";
        };
    }

    public static String getQuoteChar(String platform) {
        return switch (platform) {
            case "mariadb", "mysql" -> "`";
            // db2, mssql, oracle, postgres
            default -> "\"";
        };
    }

    public static String getColumnHash(String platform) {
        return switch (platform) {
            case "db2" -> "LOWER(HASH(%s,'MD5')) AS %s";
            case "mssql" -> "lower(convert(varchar, hashbytes('MD5',%s),2)) AS %s";
            case "oracle" -> "LOWER(STANDARD_HASH(%s,'MD5')) AS %s";
            case "mariadb", "mysql", "postgres" -> "lower(md5(%s)) AS %s";
            default -> "";
        };

    }

    public static String getConcatOperator(String platform) {
        return switch (platform) {
            case "mssql" -> "+";
            // db2, mariadb, mysql, oracle, postgres
            default -> "||";
        };
    }

    /**
     * Builds a SQL query for retrieving data from source or target.
     * @param columnHashMethod  The database hash method to use (database, hybrid, raw)
     * @param tableMap            Metadata information on table
     * @param columnMetadata      Metadata on columns
     * @return SQL query string for loading data from the specified table.
     */
    public static String buildLoadSQL (String columnHashMethod, DCTableMap tableMap, ColumnMetadata columnMetadata) {
        String platform = Props.getProperty(String.format("%s-type", tableMap.getDestType()));
        String sql = "SELECT ";

        String quoteChar = getQuoteChar(platform);

        String columnHash = getColumnHash(platform);

        switch (columnHashMethod) {
            case "raw":
            case "hybrid":
                sql += String.format("%s AS pk_hash, %s AS pk, %s ", columnMetadata.getPkExpressionList(), columnMetadata.getPkJSON(), columnMetadata.getColumnExpressionList());
                break;
            default:
                sql += String.format(columnHash, columnMetadata.getPkExpressionList(),"pk_hash, ");
                sql += String.format("%s as pk,", columnMetadata.getPkJSON());
                sql += String.format(columnHash, columnMetadata.getColumnExpressionList(),"column_hash");
                break;
        }

        sql += String.format(" FROM %s.%s WHERE 1=1",ShouldQuoteString(tableMap.isSchemaPreserveCase(), tableMap.getSchemaName(), quoteChar), ShouldQuoteString(tableMap.isTablePreserveCase(),tableMap.getTableName(), quoteChar));

        if (tableMap.getTableFilter() != null && !tableMap.getTableFilter().isEmpty()) {
            sql += " AND " + tableMap.getTableFilter();
        }

        return sql;
    }

    /**
     * Utility method to execute a provided SQL query and retrieve a list of tables.
     *
     * @param conn The database Connection object to use for executing the query.
     * @param schema The schema owner of the tables.
     * @param sql The SQL query to retrieve database version.
     * @return A JSONArray of table lists.
     */
    public static JSONArray getTables (Connection conn, String schema, String tableFilter, String sql) {
        JSONArray tableInfo = new JSONArray();

        try {
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setObject(1, schema);
            if (! tableFilter.isEmpty()) {
                stmt.setObject(2, tableFilter);
            }

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                JSONObject table = new JSONObject();
                table.put("schemaName",rs.getString("owner"));
                table.put("tableName",rs.getString("table_name"));

                tableInfo.put(table);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error retrieving tables for %s:  %s",schema,e.getMessage()));
        }

        return tableInfo;
    }

    /**
     * Utility method to execute a provided SQL query and return the database version.
     *
     * @param conn The database Connection object to use for executing the query.
     * @param sql The SQL query to retrieve database version.
     * @return A String containing the results of the query column version.
     */
    public static String getVersion (Connection conn, String sql) {
        String dbVersion = null;
        ArrayList<Object> binds = new ArrayList<>();

        try {
            CachedRowSet crsVersion = SQLService.simpleSelect(conn, sql, binds);

            if (crsVersion.next()) {
                dbVersion = crsVersion.getString("version");
            }

            crsVersion.close();

        } catch (Exception e) {
            Logging.write("info", THREAD_NAME, String.format("Could not retrieve version:  %s", e.getMessage()));
        }

        return dbVersion;
    }



}
