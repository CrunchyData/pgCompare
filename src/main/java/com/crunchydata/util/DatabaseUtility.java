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


/**
 * @author Brian Pace
 */
public class DatabaseUtility {

    public static ColumnMetadata getColumnInfo(String targetType, String platform, Connection conn, String schema, String table, Boolean useDatabaseHash) {
        JSONObject columnData = new JSONObject();

        Logging.write("info", "database-utility", "Getting columns for table " + schema + "." + table);
        JSONArray colExpression = new JSONArray();
        String concatOperator = "||";

        colExpression = switch (platform) {
            case "oracle" -> dbOracle.getColumns(conn, schema, table);
            case "mysql" -> dbMySQL.getColumns(conn, schema, table);
            case "mssql" -> {
                concatOperator = "+";
                yield dbMSSQL.getColumns(conn, schema, table);
            }
            default -> dbPostgres.getColumns(conn, schema, table);
        };

        columnData.put( (targetType.equals("source")? "sourceColumns" : "targetColumns") , colExpression);

        StringBuilder column = new StringBuilder();
        StringBuilder pk = new StringBuilder();

        StringBuilder pkList = new StringBuilder();
        StringBuilder pkJSON = new StringBuilder();
        StringBuilder columnList = new StringBuilder();
        Integer nbrColumns = 0;
        Integer nbrPKColumns = 0;

        /////////////////////////////////////////////////
        // Construct Columns
        /////////////////////////////////////////////////
        try {
            for (int i = 0; i < columnData.getJSONArray((targetType.equals("source")? "sourceColumns" : "targetColumns")).length(); i++ ) {

                JSONObject joColumn = columnData.getJSONArray((targetType.equals("source")? "sourceColumns" : "targetColumns")).getJSONObject(i);

                    if ( joColumn.getString("primaryKey").equals("N")) {
                        nbrColumns++;
                        columnList.append(joColumn.getString("columnName")).append(",");

                        column.append((useDatabaseHash) ? joColumn.getString("valueExpression")  + concatOperator : joColumn.getString("valueExpression") + " as " + joColumn.getString("columnName") + ",");

                    } else {
                        nbrPKColumns++;
                        pk.append(joColumn.getString("valueExpression")).append(concatOperator+"'.'"+concatOperator);
                        pkList.append(joColumn.getString("columnName")).append(",");

                        if (pkJSON.isEmpty()) {
                            pkJSON.append("'{'" + concatOperator);
                        } else {
                            pkJSON.append(concatOperator + " ',' " + concatOperator);
                        }

                        if (joColumn.getString("dataClass").equals("char")) {
                            pkJSON.append("'\"").append(joColumn.getString("columnName")).append("\": \"' " + concatOperator + " ").append(joColumn.getString("columnName")).append(" " + concatOperator + " '\"' ");
                        } else {
                            if ( platform.equals("mssql") ) {
                                pkJSON.append("'\"").append(joColumn.getString("columnName")).append("\": ' "   + concatOperator + " ").append("trim(cast(" + joColumn.getString("columnName") + " as varchar))");
                            } else {
                                pkJSON.append("'\"").append(joColumn.getString("columnName")).append("\": ' "   + concatOperator + " ").append(joColumn.getString("columnName"));
                            }
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
                pkJSON.append( concatOperator + "'}'");
            }
        } catch (Exception e) {
            Logging.write("severe", "database-utility", "Error while parsing column list " + e.getMessage());
            e.printStackTrace();
        }

        return new ColumnMetadata(columnList.toString(), nbrColumns, nbrPKColumns, column.toString(), pk.toString(), pkList.toString(), pkJSON.toString());

    }

}
