/*
 * Copyright 2012-2025 the original author or authors.
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

package com.crunchydata.service;

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.util.LoggingUtils;

import java.util.Objects;

import static com.crunchydata.config.Settings.Props;
import static com.crunchydata.config.sql.RepoSQLConstants.SQL_REPO_DCTABLE_SELECTBYPID;
import static com.crunchydata.util.DataProcessingUtils.ShouldQuoteString;


public class SQLSyntaxService {

    private static final String THREAD_NAME = "sql-syntax-srv";

    // SQL query constants
    private static final String SELECT_CLAUSE = "SELECT ";
    private static final String FROM_CLAUSE = " FROM ";
    private static final String WHERE_CLAUSE = " WHERE 1=1";
    private static final String AND_CLAUSE = " AND ";


    // Column hash method constants
    private static final String HASH_METHOD_RAW = "raw";
    private static final String HASH_METHOD_HYBRID = "hybrid";

    /**
     * Build SQL query for retrieving tables.
     *
     * @param batchNbr Batch number filter
     * @param table Table name filter
     * @param check Check flag for filtering results
     * @return SQL query string
     */
    public static String buildGetTablesSQL(Integer batchNbr, String table, Boolean check) {
        String sql = SQL_REPO_DCTABLE_SELECTBYPID;

        if (batchNbr > 0) {
            sql += " AND batch_nbr=?";
        }

        if (!table.isEmpty()) {
            sql += " AND table_alias=?";
        }

        if (check) {
            sql += """ 
                    AND (tid IN (SELECT tid FROM dc_target WHERE compare_result != 'e')
                         OR  tid IN (SELECT tid FROM dc_source WHERE compare_result != 'e'))
                   """;
        }

        sql += " ORDER BY table_alias";
        return sql;
    }

    /**
     * Generate compare SQL for source and target.
     *
     * @param dctmSource Source table map
     * @param dctmTarget Target table map
     * @param ciSource Source column metadata
     * @param ciTarget Target column metadata
     */
    public static void generateCompareSQL(DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                                           ColumnMetadata ciSource, ColumnMetadata ciTarget) {
        String method = Props.getProperty("column-hash-method");
        dctmSource.setCompareSQL(buildLoadSQL(method, dctmSource, ciSource));
        dctmTarget.setCompareSQL(buildLoadSQL(method, dctmTarget, ciTarget));

        LoggingUtils.write("info", THREAD_NAME, "(source) Compare SQL: " + dctmSource.getCompareSQL());
        LoggingUtils.write("info", THREAD_NAME, "(target) Compare SQL: " + dctmTarget.getCompareSQL());
    }

    /**
     * Builds a SQL query for retrieving data from source or target.
     *
     * @param columnHashMethod The database hash method to use (database, hybrid, raw)
     * @param tableMap Metadata information on table
     * @param columnMetadata Metadata on columns
     * @return SQL query string for loading data from the specified table
     * @throws IllegalArgumentException if required parameters are null or invalid
     */
    public static String buildLoadSQL(String columnHashMethod, DataComparisonTableMap tableMap, ColumnMetadata columnMetadata) {
        // Input validation
        Objects.requireNonNull(tableMap, "tableMap cannot be null");
        Objects.requireNonNull(columnMetadata, "columnMetadata cannot be null");
        Objects.requireNonNull(columnHashMethod, "columnHashMethod cannot be null");

        if (tableMap.getDestType() == null || tableMap.getDestType().trim().isEmpty()) {
            throw new IllegalArgumentException("tableMap.destType cannot be null or empty");
        }

        String platform = Props.getProperty(String.format("%s-type", tableMap.getDestType()));
        DatabaseMetadataService.DatabasePlatform dbPlatform = DatabaseMetadataService.DatabasePlatform.fromString(platform);

        StringBuilder sql = new StringBuilder(SELECT_CLAUSE);

        // Build column selection based on hash method
        switch (columnHashMethod.toLowerCase()) {
            case HASH_METHOD_RAW:
            case HASH_METHOD_HYBRID:
                sql.append(String.format("%s AS pk_hash, %s AS pk, %s ",
                        columnMetadata.getPkExpressionList(),
                        columnMetadata.getPkJSON(),
                        columnMetadata.getColumnExpressionList()));
                break;
            default:
                // Database hash method
                sql.append(String.format(dbPlatform.getColumnHashTemplate(),
                        columnMetadata.getPkExpressionList(), "pk_hash, "));
                sql.append(String.format("%s as pk,", columnMetadata.getPkJSON()));
                sql.append(String.format(dbPlatform.getColumnHashTemplate(),
                        columnMetadata.getColumnExpressionList(), "column_hash"));
                break;
        }

        // Build FROM clause with proper quoting
        String schemaName = ShouldQuoteString(tableMap.isSchemaPreserveCase(),
                tableMap.getSchemaName(), dbPlatform.getQuoteChar());
        String tableName = ShouldQuoteString(tableMap.isTablePreserveCase(),
                tableMap.getTableName(), dbPlatform.getQuoteChar());

        sql.append(FROM_CLAUSE).append(schemaName).append(".").append(tableName).append(WHERE_CLAUSE);

        // Add table filter if present
        if (tableMap.getTableFilter() != null && !tableMap.getTableFilter().trim().isEmpty()) {
            sql.append(AND_CLAUSE).append(tableMap.getTableFilter());
        }

        return sql.toString();
    }

}
