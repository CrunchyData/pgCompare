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
 * @author Brian Pace
 */
public class DatabaseUtility {

    public static ColumnMetadata getColumnInfo(JSONObject columnMap, String targetType, String platform, String schema, String table, Boolean useDatabaseHash) {
        Logging.write("info", "database-utility", "Building column expressions for " + schema + "." + table);

        String concatOperator = platform.equals("mssql") ? "+" : "||";

        StringBuilder column = new StringBuilder();
        StringBuilder pk = new StringBuilder();
        StringBuilder pkList = new StringBuilder();
        StringBuilder pkJSON = new StringBuilder();
        StringBuilder columnList = new StringBuilder();

        int nbrColumns = 0;
        Integer nbrPKColumns = 0;

        /////////////////////////////////////////////////
        // Construct Columns
        /////////////////////////////////////////////////
        try {
            for (int i = 0; i < columnMap.getJSONArray("columns").length(); i++ ) {

                if ( columnMap.getJSONArray("columns").getJSONObject(i).getString("status").equals("compare") ) {

                    JSONObject joColumn = columnMap.getJSONArray("columns").getJSONObject(i).getJSONObject(targetType);

                    if (joColumn.getBoolean("primaryKey")) {
                        nbrPKColumns++;
                        pk.append(joColumn.getString("valueExpression")).append(concatOperator).append("'.'").append(concatOperator);
                        pkList.append(joColumn.getString("columnName")).append(",");

                        if (pkJSON.isEmpty()) {
                            pkJSON.append("'{'").append(concatOperator);
                        } else {
                            pkJSON.append(concatOperator).append(" ',' ").append(concatOperator);
                        }

                        if (joColumn.getString("dataClass").equals("char")) {
                            pkJSON.append("'\"").append(joColumn.getString("columnName")).append("\": \"' ").append(concatOperator).append(" ").append(joColumn.getString("columnName")).append(" ").append(concatOperator).append(" '\"' ");
                        } else {
                            if (platform.equals("mssql")) {
                                pkJSON.append("'\"").append(joColumn.getString("columnName")).append("\": ' ").append(concatOperator).append(" ").append("trim(cast(").append(joColumn.getString("columnName")).append(" as varchar))");
                            } else {
                                pkJSON.append("'\"").append(joColumn.getString("columnName")).append("\": ' ").append(concatOperator).append(" ").append(joColumn.getString("columnName"));
                            }
                        }
                    } else {
                        nbrColumns++;
                        columnList.append(joColumn.getString("columnName")).append(",");

                        column.append((useDatabaseHash) ? joColumn.getString("valueExpression") + concatOperator : joColumn.getString("valueExpression") + " as " + joColumn.getString("columnName") + ",");
                    }
                }
            }

            if (columnList.isEmpty()) {
                column = new StringBuilder((useDatabaseHash) ? "'0'" : " '0' c1");
                nbrColumns = 1;
            } else {
                columnList = new StringBuilder(columnList.substring(0, columnList.length() - 1));
                column = new StringBuilder(column.substring(0,column.length() -  (column.substring(column.length()-1).equals("|") ? 2 : 1) ));
            }

            if ((!pk.isEmpty()) && (!pkList.isEmpty())) {
                pk = new StringBuilder(pk.substring(0, pk.length() - (3+(concatOperator.length()*2))));
                pkList = new StringBuilder(pkList.substring(0, pkList.length() - 1 ));
                pkJSON.append(concatOperator).append("'}'");
            }
        } catch (Exception e) {
            Logging.write("severe", "database-utility", "Error while parsing column list " + e.getMessage());
        }

        return new ColumnMetadata(columnList.toString(), nbrColumns, nbrPKColumns, column.toString(), pk.toString(), pkList.toString(), pkJSON.toString());

    }

    public static JSONObject getColumnMap(String targetType, String platform, Connection conn, String schema, String table, JSONObject columnData) {
        Logging.write("info", "database-utility", "Getting columns for table " + schema + "." + table);

        JSONArray colExpression = switch (platform) {
            case "oracle" -> dbOracle.getColumns(conn, schema, table);
            case "mysql" -> dbMySQL.getColumns(conn, schema, table);
            case "mssql" -> dbMSSQL.getColumns(conn, schema, table);
            default -> dbPostgres.getColumns(conn, schema, table);
        };

        JSONArray columns = new JSONArray();
        if ( columnData.has("columns") ) {
            columns = columnData.getJSONArray("columns");
        }

        int columnPosition;

        for (int i = 0; i < colExpression.length(); i++ ) {
            JSONObject columnDetail = new JSONObject();
            JSONObject findColumn = findOne(columns, "alias", colExpression.getJSONObject(i).getString("columnName"));

            if ( findColumn.getInt("count") == 0 ) {
                columnDetail.put("alias",colExpression.getJSONObject(i).getString("columnName"));
                columnDetail.put("status", "compare");
                columnPosition = -1;
            } else {
                columnPosition = findColumn.getInt("location");
                columnDetail = findColumn.getJSONObject("data");
            }

            if ( ! colExpression.getJSONObject(i).getBoolean("supported") ) {
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
