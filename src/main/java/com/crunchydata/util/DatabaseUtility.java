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

import java.sql.Connection;

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.services.dbMSSQL;
import com.crunchydata.services.dbMySQL;
import com.crunchydata.services.dbOracle;
import com.crunchydata.services.dbPostgres;

import org.json.JSONArray;
import org.json.JSONObject;

import static com.crunchydata.util.JsonUtility.findOne;


/**
 * Utility class for database operations.
 *
 * Provides methods to retrieve column metadata and map column details
 * for different database platforms.
 *
 * @author Brian Pace
 */
public class DatabaseUtility {

    private static final String THREAD_NAME = "DatabaseUtility";

    /**
     * Retrieves column metadata for a given table.
     *
     * @param columnMap        JSON object containing column mapping information
     * @param targetType       The target type of columns
     * @param platform         The database platform (e.g., "mssql", "mysql")
     * @param schema           The schema of the table
     * @param table            The name of the table
     * @param useDatabaseHash  Flag to determine whether to use database hash
     * @return                 A ColumnMetadata object containing column information
     */
    public static ColumnMetadata getColumnInfo(JSONObject columnMap, String targetType, String platform, String schema, String table, Boolean useDatabaseHash) {
        Logging.write("info", THREAD_NAME, String.format("Building column expressions for %s.%s",schema,table));

        // Variables
        StringBuilder column = new StringBuilder();
        StringBuilder columnList = new StringBuilder();
        String concatOperator = platform.equals("mssql") ? "+" : "||";
        int nbrColumns = 0;
        Integer nbrPKColumns = 0;
        StringBuilder pk = new StringBuilder();
        StringBuilder pkJSON = new StringBuilder();
        StringBuilder pkList = new StringBuilder();

        // Construct Columns
        try {
            JSONArray columnsArray = columnMap.getJSONArray("columns");
            for (int i = 0; i < columnsArray.length(); i++) {
                JSONObject columnObject = columnsArray.getJSONObject(i);

                if ("compare".equals(columnObject.getString("status"))) {
                    JSONObject joColumn = columnObject.getJSONObject(targetType);

                    if (joColumn.getBoolean("primaryKey")) {
                        nbrPKColumns++;
                        pk.append(joColumn.getString("valueExpression"))
                                .append(concatOperator)
                                .append("'.'")
                                .append(concatOperator);
                        pkList.append(joColumn.getString("columnName")).append(",");

                        if (pkJSON.isEmpty()) {
                            pkJSON.append("'{'").append(concatOperator);
                        } else {
                            pkJSON.append(concatOperator).append(" ',' ").append(concatOperator);
                        }

                        if ("char".equals(joColumn.getString("dataClass"))) {
                            pkJSON.append("'\"").append(joColumn.getString("columnName")).append("\": \"' ")
                                    .append(concatOperator).append(" ").append(joColumn.getString("columnName"))
                                    .append(" ").append(concatOperator).append(" '\"' ");
                        } else {
                            if (platform.equals("mssql")) {
                                pkJSON.append("'\"").append(joColumn.getString("columnName")).append("\": ' ")
                                        .append(concatOperator).append(" ").append("trim(cast(")
                                        .append(joColumn.getString("columnName")).append(" as varchar))");
                            } else {
                                pkJSON.append("'\"").append(joColumn.getString("columnName")).append("\": ' ")
                                        .append(concatOperator).append(" ").append(joColumn.getString("columnName"));
                            }
                        }
                    } else {
                        nbrColumns++;
                        columnList.append(joColumn.getString("columnName")).append(",");
                        column.append(useDatabaseHash
                                ? joColumn.getString("valueExpression") + concatOperator
                                : joColumn.getString("valueExpression") + " as " + joColumn.getString("columnName") + ",");
                    }
                }
            }


            if (columnList.isEmpty()) {
                column = new StringBuilder((useDatabaseHash) ? "'0'" : " '0' c1");
                nbrColumns = 1;
            } else {
                columnList.setLength(columnList.length() - 1);
                column.setLength(column.length() - (column.substring(column.length() - 1).equals("|") ? 2 : 1));
            }

            if (!pk.isEmpty() && !pkList.isEmpty()) {
                pk.setLength(pk.length() - (3 + (concatOperator.length() * 2)));
                pkList.setLength(pkList.length() - 1);
                pkJSON.append(concatOperator).append("'}'");
            }

        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error while parsing column list:  %s",e.getMessage()));
        }

        return new ColumnMetadata(columnList.toString(), nbrColumns, nbrPKColumns, column.toString(), pk.toString(), pkList.toString(), pkJSON.toString());

    }

    /**
     * Retrieves column details for a given table and maps the columns.
     *
     * @param targetType  The target type of columns
     * @param platform    The database platform (e.g., "oracle", "mysql")
     * @param conn        The database connection
     * @param schema      The schema of the table
     * @param table       The name of the table
     * @param columnData  JSON object containing column data
     * @return            A JSON object with updated column mappings
     */
    public static JSONObject getColumnMap(String targetType, String platform, Connection conn, String schema, String table, JSONObject columnData) {
        Logging.write("info", THREAD_NAME, String.format("Getting columns for table %s.%s",schema,table));

        JSONArray colExpression = switch (platform) {
            case "oracle" -> dbOracle.getColumns(conn, schema, table);
            case "mysql" -> dbMySQL.getColumns(conn, schema, table);
            case "mssql" -> dbMSSQL.getColumns(conn, schema, table);
            default -> dbPostgres.getColumns(conn, schema, table);
        };

        JSONArray columns = columnData.optJSONArray("columns") != null ? columnData.getJSONArray("columns") : new JSONArray();

        if ( columnData.has("columns") ) {
            columns = columnData.getJSONArray("columns");
        }


        for (int i = 0; i < colExpression.length(); i++) {
            JSONObject columnDetail = new JSONObject();
            JSONObject findColumn = findOne(columns, "alias", colExpression.getJSONObject(i).getString("columnName"));

            int columnPosition = findColumn.getInt("count") == 0 ? -1 : findColumn.getInt("location");

            if (columnPosition == -1) {
                columnDetail.put("alias", colExpression.getJSONObject(i).getString("columnName"));
                columnDetail.put("status", "compare");
            } else {
                columnDetail = findColumn.getJSONObject("data");
            }

            if (!colExpression.getJSONObject(i).getBoolean("supported")) {
                columnDetail.put("status", "ignore");
            }

            columnDetail.put(targetType, colExpression.getJSONObject(i));

            if (columnPosition == -1) {
                columns.put(columnDetail);
            } else {
                columns.put(columnPosition, columnDetail);
            }
        }

        columnData.put("columns", columns);

        return columnData;

    }

}
