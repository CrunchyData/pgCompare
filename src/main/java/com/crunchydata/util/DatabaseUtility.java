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

import com.crunchydata.model.ColumnInfo;
import com.crunchydata.services.dbOracle;
import com.crunchydata.services.dbPostgres;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;

/**
 * @author Brian Pace
 */
public class DatabaseUtility {

    public static ColumnInfo getColumnInfo(String targetType, Connection conn, String targetSchema, String targetTable, Boolean useDatabaseHash) {

        CachedRowSet crsColumns;

        Logging.write("info", "reconcile-controller", "Getting columns for table " + targetSchema + "." + targetTable);

        crsColumns = ( targetType.equals("postgres")) ? dbPostgres.getColumns(conn, targetSchema, targetTable) : dbOracle.getColumns(conn, targetSchema, targetTable);

        StringBuilder oraColumn = new StringBuilder();
        StringBuilder pgColumn = new StringBuilder();
        StringBuilder oraPK = new StringBuilder();
        StringBuilder pgPK = new StringBuilder();
        StringBuilder pkList = new StringBuilder();
        StringBuilder pkJSON = new StringBuilder();
        String oraTemp ="";
        String pgTemp ="";
        StringBuilder columnList = new StringBuilder();
        Integer nbrColumns = 0;
        Integer nbrPKColumns = 0;
        boolean dtSupported;

        /////////////////////////////////////////////////
        // Construct Columns
        /////////////////////////////////////////////////
        try {
            while (crsColumns.next()) {
                dtSupported = true;
                String dt = "char";
                columnList.append(crsColumns.getString("column_name")).append(",");
                switch (crsColumns.getString("data_type")) {
                    case "bool":
                    case "BOOLEAN":
                        dt="boolean";
                        oraTemp = "nvl(to_char("+crsColumns.getString("column_name")+"),'0')";
                        pgTemp = "case when coalesce(" +crsColumns.getString("column_name") + "::text,'0') = 'true' then '1' else '0' end";
                        break;
                    case "int2":
                    case "int4":
                    case "int8":
                        dt="numeric";
                        oraTemp = "nvl(to_char("+crsColumns.getString("column_name")+"),' ')";
                        pgTemp = "coalesce(" +crsColumns.getString("column_name") + "::text,' ')";
                        break;
                    case "NUMBER":
                    case "BINARY_DOUBLE":
                    case "BINARY_FLOAT":
                    case "FLOAT":
                    case "numeric":
                        dt="numeric";
                        oraTemp = "nvl(to_char("+crsColumns.getString("column_name")+",'0000000000000000000000.0000000000000000000000'),' ')";
                        pgTemp  = "coalesce(to_char(trim_scale("+crsColumns.getString("column_name")+"),'0000000000000000000000.0000000000000000000000'),' ')";
                        break;
                    case "DATE":
                    case "TIMESTAMP":
                    case "TIMESTAMP(0)":
                    case "TIMESTAMP(1) WITH TIME ZONE":
                    case "TIMESTAMP(2)":
                    case "TIMESTAMP(3)":
                    case "TIMESTAMP(3) WITH TIME ZONE":
                    case "TIMESTAMP(6)":
                    case "TIMESTAMP(6) WITH TIME ZONE":
                    case "TIMESTAMP(9)":
                    case "TIMESTAMP(9) WITH TIME ZONE":
                    case "timestamp":
                        oraTemp = "nvl(to_char("+crsColumns.getString("column_name")+",'MMDDYYYYHH24MISS'),' ')";
                        pgTemp  = "coalesce(to_char("+crsColumns.getString("column_name")+",'MMDDYYYYHH24MISS'),' ')";
                        break;
                    case "CHAR":
                    case "bpchar":
                        if (crsColumns.getInt("data_length") > 1) {
                            oraTemp = "nvl(trim("+crsColumns.getString("column_name")+"),' ')";
                        } else {
                            oraTemp = "nvl("+crsColumns.getString("column_name")+",' ')";
                        }
                        pgTemp  = "coalesce("+crsColumns.getString("column_name")+"::text,' ')";
                        break;
                    case "text":
                    case "VARCHAR2":
                    case "varchar":
                        oraTemp = "nvl("+crsColumns.getString("column_name")+",' ')";
                        pgTemp  = "coalesce("+crsColumns.getString("column_name")+"::text,' ')";
                        break;
                    default:
                        dtSupported = false;
                }

                if (dtSupported) {
                    nbrColumns++;
                    if (crsColumns.getString("pk").equals("N")) {
                        oraColumn.append((useDatabaseHash) ? oraTemp  + "||" : oraTemp + " as " + crsColumns.getString("column_name") + ",");
                        pgColumn.append((useDatabaseHash) ? pgTemp + "," : pgTemp + " as " + crsColumns.getString("column_name") + ",");
                    } else {
                        nbrPKColumns++;
                        oraPK.append(oraTemp).append("||'.'||");
                        pgPK.append(pgTemp).append("||'.'||");
                        pkList.append(crsColumns.getString("column_name")).append(",");
                        if (dt.equals("char")) {
                            pkJSON.append("'\"").append(crsColumns.getString("column_name")).append("\": \"' || ").append(crsColumns.getString("column_name")).append(" || '\",' ||");
                        } else {
                            pkJSON.append("'\"").append(crsColumns.getString("column_name")).append("\": ' || ").append(crsColumns.getString("column_name")).append(" || ',' ||");
                        }
                    }
                } else {
                    Logging.write("warning", "reconcile-controller", "Unsupported data type (" + crsColumns.getString("data_type") + ") for column " + crsColumns.getString("column_name"));
                }
            }
            columnList = new StringBuilder(columnList.substring(0, columnList.length() - 1));

            if (oraColumn.isEmpty()) {
                oraColumn = new StringBuilder((useDatabaseHash) ? "'0'" : " '0' c1");
                pgColumn = new StringBuilder((useDatabaseHash) ? "'0'" : " '0' c1");
            } else {
                oraColumn = new StringBuilder(oraColumn.substring(0, oraColumn.length() - ((useDatabaseHash) ? 2 : 1)));
                pgColumn = new StringBuilder(pgColumn.substring(0, pgColumn.length() - 1));
            }

            if ((!pgPK.isEmpty()) && (!pkList.isEmpty())) {
                oraPK = new StringBuilder(oraPK.substring(0, oraPK.length() - 7));
                pgPK = new StringBuilder(pgPK.substring(0, pgPK.length() - 7));
                pkList = new StringBuilder(pkList.substring(0, pkList.length() - 1));
                pkJSON = new StringBuilder("'{' || " + pkJSON.substring(0, pkJSON.length() - 5) + "' || '}'");
            }
        } catch (Exception e) {
            Logging.write("severe", "reconcile-controller", "Error while parsing column list " + e.getMessage());
        }

        return new ColumnInfo(columnList.toString(), nbrColumns, nbrPKColumns, oraColumn.toString(), oraPK.toString(), pgColumn.toString(), pgPK.toString(), pkList.toString(), pkJSON.toString());

    }


}
