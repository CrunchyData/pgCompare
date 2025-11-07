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

package com.crunchydata.core.database;

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

import static com.crunchydata.util.DataTypeCastingUtils.cast;
import static com.crunchydata.util.DataTypeCastingUtils.castRaw;
import static com.crunchydata.util.DataProcessingUtils.ShouldQuoteString;
import static com.crunchydata.util.JsonProcessingUtils.buildJsonExpression;
import static com.crunchydata.config.Settings.Props;

/**
 * Builder class for constructing ColumnMetadata objects from JSON column mappings.
 * This class handles the complex logic of parsing column information and building
 * SQL expressions for database operations.
 * 
 * @author Brian Pace
 */
public class ColumnMetadataBuilder {
    
    private static final String THREAD_NAME = "column-metadata-builder";
    
    // Configuration
    private final String targetType;
    private final String platform;
    private final String schema;
    private final String table;
    private final boolean useDatabaseHash;
    private final String concatOperator;
    private final String quoteChar;
    private final String replaceSyntax;

    // Column collections
    private final List<String> pkList = new ArrayList<>();
    private final List<String> columnList = new ArrayList<>();
    private final List<String> columnExpressionList = new ArrayList<>();
    private final List<String> pkHash = new ArrayList<>();
    private final List<String> pkJSON = new ArrayList<>();
    
    // Counters
    private int nbrColumns = 0;
    private int nbrPKColumns = 0;
    
    /**
     * Constructor for ColumnMetadataBuilder.
     * 
     * @param targetType The target type of columns
     * @param platform The database platform
     * @param schema The schema name
     * @param table The table name
     * @param useDatabaseHash Whether to use database hash
     * @param concatOperator The concatenation operator for the platform
     * @param quoteChar The quote character for the platform
     */
    public ColumnMetadataBuilder(String targetType, String platform, String schema, String table, 
                                boolean useDatabaseHash, String concatOperator, String quoteChar, String replaceSyntax) {
        this.targetType = targetType;
        this.platform = platform;
        this.schema = schema;
        this.table = table;
        this.useDatabaseHash = useDatabaseHash;
        this.concatOperator = concatOperator;
        this.quoteChar = quoteChar;
        this.replaceSyntax = replaceSyntax;
    }
    
    /**
     * Build ColumnMetadata from JSON column mapping.
     * 
     * @param columnMap JSON object containing column mapping information
     * @return ColumnMetadata object
     */
    public ColumnMetadata build(JSONObject columnMap) {
        LoggingUtils.write("info", THREAD_NAME,
            String.format("(%s) Building column expressions for %s.%s", targetType, schema, table));
        
        try {
            processColumns(columnMap);
            return createColumnMetadata();
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Error building column metadata: %s", e.getMessage()));
            throw new RuntimeException("Failed to build column metadata", e);
        }
    }
    
    /**
     * Process all columns from the column mapping.
     * 
     * @param columnMap JSON object containing column mappings
     */
    private void processColumns(JSONObject columnMap) {
        JSONArray columnsArray = columnMap.getJSONArray("columns");
        
        for (int i = 0; i < columnsArray.length(); i++) {
            JSONObject columnObject = columnsArray.getJSONObject(i);
            
            if (columnObject.getBoolean("enabled")) {
                processEnabledColumn(columnObject);
            } else {
                logSkippedColumn(columnObject);
            }
        }
        
        handleEmptyColumnList();
    }
    
    /**
     * Process a single enabled column.
     * 
     * @param columnObject JSON object representing the column
     */
    private void processEnabledColumn(JSONObject columnObject) {
        JSONObject joColumn = columnObject.getJSONObject(targetType);
        
        // Extract column information
        String columnName = buildQuotedColumnName(joColumn);
        String dataType = joColumn.getString("dataType").toLowerCase();
        String dataClass = joColumn.getString("dataClass");
        
        // Generate or use custom value expression
        String valueExpression = generateValueExpression(joColumn, columnName, dataType);
        joColumn.put("valueExpression", valueExpression);
        
        LoggingUtils.write("debug", THREAD_NAME,
            String.format("(%s) Mapping expression for column %s: %s", 
                targetType, columnObject.getString("columnAlias"), valueExpression));
        
        // Process based on whether it's a primary key
        if (joColumn.getBoolean("primaryKey")) {
            processPrimaryKeyColumn(joColumn, columnName, dataClass);
        } else {
            processRegularColumn(joColumn, columnName);
        }
    }
    
    /**
     * Process a primary key column.
     * 
     * @param joColumn Column JSON object
     * @param columnName Quoted column name
     * @param dataClass Data class of the column
     */
    private void processPrimaryKeyColumn(JSONObject joColumn, String columnName, String dataClass) {
        nbrPKColumns++;
        
        // Add to primary key collections
        pkHash.add(joColumn.getString("valueExpression"));
        pkList.add(columnName);
        pkJSON.add(buildJsonExpression(platform, columnName, dataClass, concatOperator, replaceSyntax));
    }
    
    /**
     * Process a regular (non-primary key) column.
     * 
     * @param joColumn Column JSON object
     * @param columnName Quoted column name
     */
    private void processRegularColumn(JSONObject joColumn, String columnName) {
        nbrColumns++;
        columnList.add(columnName);
        
        String expression = useDatabaseHash 
            ? joColumn.getString("valueExpression")
            : joColumn.getString("valueExpression") + " as " + joColumn.getString("columnName").toLowerCase();
        columnExpressionList.add(expression);
    }
    
    /**
     * Build quoted column name based on preserve case setting.
     * 
     * @param joColumn Column JSON object
     * @return Quoted column name
     */
    private String buildQuotedColumnName(JSONObject joColumn) {
        return ShouldQuoteString(
            joColumn.getBoolean("preserveCase"),
            joColumn.getString("columnName"),
            quoteChar
        );
    }
    
    /**
     * Generate value expression for the column.
     * 
     * @param joColumn Column JSON object
     * @param columnName Column name
     * @param dataType Data type
     * @return Value expression string
     */
    private String generateValueExpression(JSONObject joColumn, String columnName, String dataType) {
        // Check if custom expression is provided
        if (!joColumn.isNull("valueExpression") && !joColumn.getString("valueExpression").isEmpty()) {
            LoggingUtils.write("info", THREAD_NAME,
                String.format("(%s) Using custom column expression for column %s: %s", 
                    targetType, joColumn.getString("columnName"), joColumn.getString("valueExpression")));
            return joColumn.getString("valueExpression");
        }
        
        // Generate default expression based on hash method
        String columnHashMethod = Props.getProperty("column-hash-method");
        return "raw".equals(columnHashMethod)
            ? castRaw(dataType, columnName, platform)
            : cast(dataType, columnName, platform, joColumn);
    }
    
    /**
     * Log information about a skipped column.
     * 
     * @param columnObject Column JSON object
     */
    private void logSkippedColumn(JSONObject columnObject) {
        LoggingUtils.write("warning", THREAD_NAME,
            String.format("Skipping disabled column: %s", columnObject.getString("columnAlias")));
    }
    
    /**
     * Handle case where no columns are found.
     */
    private void handleEmptyColumnList() {
        if (columnList.isEmpty()) {
            columnExpressionList.add(useDatabaseHash ? "'0'" : "'0' as c1");
            nbrColumns = 1;
        }
    }
    
    /**
     * Create the final ColumnMetadata object.
     * 
     * @return ColumnMetadata object
     */
    private ColumnMetadata createColumnMetadata() {
        String finalPkList = String.join(",", pkList);
        String finalColumnList = String.join(",", columnList);
        String finalPkHash = buildFinalPkHash();
        String finalPkJson = buildFinalPkJson();
        String finalColumnExpressionList = buildFinalColumnExpression();
        
        return new ColumnMetadata(
            finalColumnList, 
            nbrColumns, 
            nbrPKColumns, 
            finalColumnExpressionList, 
            finalPkHash, 
            finalPkList, 
            finalPkJson
        );
    }
    
    /**
     * Build final primary key hash expression.
     * 
     * @return Primary key hash expression
     */
    private String buildFinalPkHash() {
        if (pkHash.isEmpty()) return "";
        
        String joined = String.join(
            platform.equals("db2") || platform.equals("oracle") 
                ? concatOperator + "'.'" + concatOperator 
                : ",'.',", 
            pkHash
        );
        
        if (platform.equals("postgres") || platform.equals("mariadb") || 
            platform.equals("mssql") || platform.equals("mysql") || platform.equals("snowflake")) {
            return pkHash.size() > 1 ? "concat(" + joined + ")" : joined;
        }
        
        return joined;
    }
    
    /**
     * Build final primary key JSON expression.
     * 
     * @return Primary key JSON expression
     */
    private String buildFinalPkJson() {
        if (pkJSON.isEmpty()) return "";
        
        String joined = String.join(concatOperator + " ',' " + concatOperator, pkJSON);
        String fullExpr = "'{'" + concatOperator + joined + concatOperator + "'}'";
        
        // Handle MariaDB special case
        if (platform.equals("mariadb")) {
            return "concat(" + fullExpr.replace("||", ",") + ")";
        }
        
        return fullExpr;
    }
    
    /**
     * Build final column expression list.
     * 
     * @return Column expression list
     */
    private String buildFinalColumnExpression() {
        if (columnExpressionList.isEmpty()) {
            return useDatabaseHash ? "'0'" : "'0' c1";
        }
        
        return String.join(useDatabaseHash ? concatOperator : ",", columnExpressionList);
    }
}
