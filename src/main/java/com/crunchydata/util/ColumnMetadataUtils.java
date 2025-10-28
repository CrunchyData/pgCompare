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

import com.crunchydata.service.*;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

import static com.crunchydata.core.database.SnowflakeHelper.GetSnowflakePrimaryKey;
import static com.crunchydata.service.DatabaseMetadataService.getNativeCase;
import static com.crunchydata.service.DatabaseMetadataService.getQuoteChar;
import static com.crunchydata.util.DataTypeCastingUtils.cast;
import static com.crunchydata.util.DataTypeCastingUtils.castRaw;
import static com.crunchydata.util.DataProcessingUtils.*;
import static com.crunchydata.config.sql.DB2SQLConstants.SQL_DB2_SELECT_COLUMNS;
import static com.crunchydata.config.sql.MSSQLSQLConstants.SQL_MSSQL_SELECT_COLUMNS;
import static com.crunchydata.config.sql.MYSQLSQLConstants.SQL_MYSQL_SELECT_COLUMNS;
import static com.crunchydata.config.sql.MariaDBSQLConstants.SQL_MARIADB_SELECT_COLUMNS;
import static com.crunchydata.config.sql.OracleSQLConstants.SQL_ORACLE_SELECT_COLUMNS;
import static com.crunchydata.config.sql.PostgresSQLConstants.SQL_POSTGRES_SELECT_COLUMNS;
import static com.crunchydata.config.sql.SnowflakeSQLConstants.SQL_SNOWFLAKE_SELECT_COLUMNS;
import static com.crunchydata.config.sql.RepoSQLConstants.SQL_REPO_DCTABLECOLUMNMAP_BYORIGINALIAS;

/**
 * Utility class for column data type validation and classification.
 * Provides methods to determine the classification of a given database column data type.
 *
 * <p>This class handles column metadata retrieval, data type classification, and column filtering
 * across different database platforms including PostgreSQL, Oracle, MySQL, MariaDB, MSSQL, Snowflake, and DB2.</p>
 *
 * @author Brian Pace
 */
public class ColumnMetadataUtils {

    private static final String THREAD_NAME = "column-util";
    
    // Constants for better maintainability
    private static final String SUPPORTED_PROPERTY = "supported";
    private static final String COLUMN_NAME_PROPERTY = "columnName";
    private static final String DATA_TYPE_PROPERTY = "dataType";
    private static final String DATA_LENGTH_PROPERTY = "dataLength";
    private static final String DATA_PRECISION_PROPERTY = "dataPrecision";
    private static final String DATA_SCALE_PROPERTY = "dataScale";
    private static final String NULLABLE_PROPERTY = "nullable";
    private static final String PRIMARY_KEY_PROPERTY = "primaryKey";
    private static final String DATA_CLASS_PROPERTY = "dataClass";
    private static final String PRESERVE_CASE_PROPERTY = "preserveCase";
    private static final String VALUE_EXPRESSION_PROPERTY = "valueExpression";
    private static final String RAW_HASH_METHOD = "raw";
    private static final String Y_VALUE = "Y";
    private static final String CHAR_DATA_CLASS = "char";
    private static final String BOOLEAN_DATA_CLASS = "boolean";
    private static final String NUMERIC_DATA_CLASS = "numeric";

    /**
     * BOOLEAN data types
     */
    public static final Set<String> BOOLEAN_TYPES = Set.of("bool", "boolean");

    /**
     * STRING data types
     */
    public static final Set<String> STRING_TYPES = Set.of("bpchar", "char", "character", "clob", "enum", "json", "jsonb", "nchar", "nclob",
            "ntext", "nvarchar", "nvarchar2", "text", "varchar", "varchar2", "xml");

    /**
     * NUMERIC data types
     */
    public static final Set<String> NUMERIC_TYPES = Set.of(
            "bigint", "bigserial", "binary_double", "binary_float", "dec",
            "decimal", "double", "double precision", "fixed", "float", "float4",
            "float8", "int", "integer", "int2", "int4", "int8", "money", "number",
            "numeric", "real", "serial", "smallint", "smallmoney", "smallserial", "tinyint"
    );


    /**
     * TIMESTAMP data types
     */
    public static final Set<String> TIMESTAMP_TYPES = Set.of("date", "datetime", "datetimeoffset", "datetime2",
            "smalldatetime", "time", "timestamp", "timestamptz", "timestamp(0)", "timestamp(1) with time zone",
            "timestamp(3)", "timestamp(3) with time zone", "timestamp(6)", "timestamp(6) with time zone",
            "timestamp(9)", "timestamp(9) with time zone", "year");

    /**
     * BINARY data types
     */
    public static final Set<String> BINARY_TYPES = Set.of("bytea", "binary", "blob", "raw", "varbinary");

    /**
     * Unsupported data types
     */
    public static final Set<String> UNSUPPORTED_TYPES = Set.of("bfile", "bit", "cursor", "hierarchyid",
            "image", "rowid", "rowversion", "set", "sql_variant", "uniqueidentifier", "long", "long raw");

    /**
     * Reserved Words
     */
    public static final Set<String> RESERVED_WORDS = Set.of("add", "all", "alter", "and", "any", "as", "asc", "at", "authid", "between", "by",
            "character", "check", "cluster", "column", "comment", "connect", "constraint", "continue",
            "create", "cross", "current", "current_user", "cursor", "database", "date", "default",
            "delete", "desc", "distinct", "double", "else", "end", "except", "exception", "exists",
            "external", "fetch", "for", "from", "grant", "group", "having", "identified", "if", "in",
            "index", "insert", "integer", "intersect", "into", "is", "join", "like", "lock", "long", "loop",
            "modify", "natural", "no", "not", "null", "on", "open", "option", "or", "order", "outer",
            "package", "prior", "privileges", "procedure", "public", "rename", "replace", "rowid", "rownum",
            "schema", "select", "session", "set", "sql", "start", "statement", "sys", "table", "then", "to",
            "trigger", "union", "unique", "update", "values", "view", "varchar", "varchar2", "when",
            "where", "with", "xor", "user");

    /**
     * Creates a column filter clause for database queries based on table and column metadata.
     *
     * @param repoConn Repository database connection
     * @param tid Table identifier
     * @param columnAlias Column alias to filter by
     * @param destRole Destination role (source/target)
     * @param quoteChar Quote character for the database platform
     * @return SQL filter clause string
     */
    public static String createColumnFilterClause(Connection repoConn, Integer tid, String columnAlias, String destRole, String quoteChar) {
        String columnFilter = "";
        ArrayList<Object> binds = new ArrayList<>();

        binds.add(0, tid);
        binds.add(1, columnAlias);
        binds.add(2, destRole);

        CachedRowSet crs = SQLExecutionService.simpleSelect(repoConn, SQL_REPO_DCTABLECOLUMNMAP_BYORIGINALIAS, binds);

        try {
            while (crs.next()) {
                columnFilter = " AND " + ShouldQuoteString(crs.getBoolean("preserve_case"), crs.getString("column_name"), quoteChar) + " = ?";
            }

            crs.close();

        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Error locating primary key column map: %s", e.getMessage()));
        }

        return columnFilter;
    }

    /**
     * Finds a column alias within a JSON array of columns based on column name and side.
     *
     * @param columns JSON array containing column metadata
     * @param columnNameToFind The column name to search for
     * @param side The side to search (source/target)
     * @return The column alias if found, null otherwise
     */
    public static String findColumnAlias(JSONArray columns, String columnNameToFind, String side) {
        for (int i = 0; i < columns.length(); i++) {
            JSONObject columnObj = columns.getJSONObject(i);
            JSONObject sideObj = columnObj.optJSONObject(side);

            if (sideObj != null && columnNameToFind.equalsIgnoreCase(sideObj.optString("columnName"))) {
                return columnObj.optString("columnAlias");
            }
        }
        return null; // Not found
    }

    /**
     * Retrieves column metadata for a specified table in Postgres database.
     *
     * @param conn      Database connection to Postgres server.
     * @param schema    Schema name of the table.
     * @param table     Table name.
     * @param destRole  Role of the database (source/target)
     * @return JSONArray containing metadata for each column in the table.
     */
    public static JSONArray getColumns (Properties Props, Connection conn, String schema, String table, String destRole) {
        JSONArray columnInfo = new JSONArray();
        String platform = Props.getProperty(destRole + "-type");
        String dbname = Props.getProperty(destRole + "-dbname");
        String nativeCase = getNativeCase(platform);
        String quoteChar = getQuoteChar(platform);

        String columnSQL = switch (platform) {
            case "oracle" -> SQL_ORACLE_SELECT_COLUMNS;
            case "mariadb" -> SQL_MARIADB_SELECT_COLUMNS;
            case "mysql" -> SQL_MYSQL_SELECT_COLUMNS;
            case "mssql" -> SQL_MSSQL_SELECT_COLUMNS;
            case "db2" -> SQL_DB2_SELECT_COLUMNS;
            case "snowflake" -> SQL_SNOWFLAKE_SELECT_COLUMNS;
            default -> SQL_POSTGRES_SELECT_COLUMNS;
        };

        try (PreparedStatement stmt = conn.prepareStatement(columnSQL)) {
            stmt.setObject(1, schema);
            stmt.setObject(2, table);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    JSONObject column = buildColumnMetadata(rs, nativeCase, quoteChar, platform, Props);
                    columnInfo.put(column);
                }

                columnInfo = (platform.equals("snowflake") ? GetSnowflakePrimaryKey(conn, columnInfo, dbname, schema, table) : columnInfo);

            }
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Error retrieving columns for table %s.%s: %s", schema, table, e.getMessage()));
        }
        return columnInfo;
    }

    /**
     * Returns the classification of a given database column data type.
     *
     * <p>The method checks the provided dataType against predefined arrays of data types and returns
     * a classification string.</p>
     *
     * <p>If the dataType does not match any predefined type, it defaults to "char".</p>
     *
     * @param dataType The database column data type to classify.
     * @return A string representing the classification of the data type ("boolean", "numeric", "char").
     */
    public static String getDataClass(String dataType) {
        // Default classification is "char"
        String dataClass = CHAR_DATA_CLASS;
        String dataTypeLower = dataType.toLowerCase();

        if (BOOLEAN_TYPES.contains(dataTypeLower)) {
            dataClass = BOOLEAN_DATA_CLASS;
        } else if (NUMERIC_TYPES.contains(dataTypeLower)) {
            dataClass = NUMERIC_DATA_CLASS;
        } else if (TIMESTAMP_TYPES.contains(dataTypeLower)) {
            dataClass = CHAR_DATA_CLASS;
        }

        return dataClass;
    }
    
    /**
     * Builds column metadata JSON object from ResultSet.
     *
     * @param rs ResultSet containing column information
     * @param nativeCase Native case setting for the platform
     * @param quoteChar Quote character for the platform
     * @param platform Database platform
     * @param Props Properties object containing configuration
     * @return JSONObject containing column metadata
     * @throws Exception if an error occurs processing the column data
     */
    private static JSONObject buildColumnMetadata(ResultSet rs, String nativeCase, String quoteChar, String platform, Properties Props) throws Exception {
        JSONObject column = new JSONObject();
        String dataType = rs.getString("data_type");
        String columnName = rs.getString("column_name");
        
        // Check for unsupported data types
        if (UNSUPPORTED_TYPES.contains(dataType.toLowerCase())) {
            LoggingUtils.write("warning", THREAD_NAME, String.format("Unsupported data type (%s) for column (%s)", dataType, columnName));
            column.put(SUPPORTED_PROPERTY, false);
        } else {
            column.put(SUPPORTED_PROPERTY, true);
        }
        
        // Set basic column properties
        column.put(COLUMN_NAME_PROPERTY, columnName);
        column.put(DATA_TYPE_PROPERTY, dataType);
        column.put(DATA_LENGTH_PROPERTY, rs.getInt("data_length"));
        column.put(DATA_PRECISION_PROPERTY, rs.getInt("data_precision"));
        column.put(DATA_SCALE_PROPERTY, rs.getInt("data_scale"));
        column.put(NULLABLE_PROPERTY, rs.getString("nullable").equals(Y_VALUE));
        column.put(PRIMARY_KEY_PROPERTY, rs.getString("pk").equals(Y_VALUE));
        column.put(DATA_CLASS_PROPERTY, getDataClass(dataType.toLowerCase()));
        column.put(PRESERVE_CASE_PROPERTY, preserveCase(nativeCase, columnName));

        // Build quoted column name
        String quotedColumnName = ShouldQuoteString(
                column.getBoolean(PRESERVE_CASE_PROPERTY),
                column.getString(COLUMN_NAME_PROPERTY),
                quoteChar
        );

        // Set value expression based on hash method
        String columnHashMethod = Props.getProperty("column-hash-method");
        String dataTypeLower = dataType.toLowerCase();
        
        column.put(VALUE_EXPRESSION_PROPERTY, RAW_HASH_METHOD.equals(columnHashMethod)
                ? castRaw(dataTypeLower, quotedColumnName, platform)
                : cast(dataTypeLower, quotedColumnName, platform, column));

        return column;
    }

}

