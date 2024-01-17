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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.serial.SerialClob;

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataCompare;
import com.crunchydata.util.DataUtility;
import com.crunchydata.util.Logging;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * @author Brian Pace
 */
public class dbReconcileCheck {

    public static void reCheck (Connection repoConn, Connection sourceConn, Connection targetConn, String sourceSQL, String targetSQL, String tableFilter, String pkList, ArrayList<Object> binds, DataCompare dcRow, Integer cid) {
        JSONObject rowResult = new JSONObject();
        JSONArray arr = new JSONArray();
        int columnOutofSync = 0;
        rowResult.put("compareStatus","in-sync");
        rowResult.put("equal",0);
        rowResult.put("notEqual",0);
        rowResult.put("missingSource",0);
        rowResult.put("missingTarget",0);

        String sqlUpdateCount = """
                                 UPDATE dc_result SET equal_cnt=equal_cnt+?, source_cnt=source_cnt+?, target_cnt=target_cnt+?
                                 WHERE cid=?
                                 """;

        CachedRowSet sourceRow = dbPostgres.simpleSelect(sourceConn, sourceSQL + tableFilter, binds);
        CachedRowSet targetRow = dbPostgres.simpleSelect(targetConn, targetSQL + tableFilter, binds);

        try {
            if (sourceRow.size() > 0 && targetRow.size() == 0) {
                rowResult.put("compareStatus", "out-of-sync");
                rowResult.put("missingTarget", 1);
                rowResult.put("result", new JSONArray().put(0, "Missing Target"));
            } else if (targetRow.size() > 0 && sourceRow.size() == 0 ) {
                rowResult.put("compareStatus", "out-of-sync");
                rowResult.put("missingSource", 1);
                rowResult.put("result", new JSONArray().put(0, "Missing Source"));
            } else {

                RowSetMetaData rowMetadata = (RowSetMetaData) sourceRow.getMetaData();
                sourceRow.next();
                targetRow.next();
                for (int i = 2; i <= rowMetadata.getColumnCount(); i++) {
                    String column = rowMetadata.getColumnName(i);
                    String sourceValue = (sourceRow.getString(i).contains("javax.sql.rowset.serial.SerialClob")) ? DataUtility.clobToString((SerialClob) sourceRow.getObject(i)) : sourceRow.getString(i);
                    String targetValue = (targetRow.getString(i).contains("javax.sql.rowset.serial.SerialClob")) ? DataUtility.clobToString((SerialClob) targetRow.getObject(i)) : targetRow.getString(i);

                    if (!sourceValue.equals(targetValue)) {
                        JSONObject col = new JSONObject();
                        String jsonString = "{ source: " + sourceValue + ", target: " + ((targetValue.equals(" ")) ? "\" \"" : targetValue) + "}";
                        col.put(column, new JSONObject(jsonString));
                        arr.put(columnOutofSync, col);
                        columnOutofSync++;
                    }
                }

                if (columnOutofSync > 0) {
                    rowResult.put("compareStatus", "out-of-sync");
                    rowResult.put("notEqual", 1);
                    rowResult.put("result", arr);
                }

            }

            if (rowResult.get("compareStatus").equals("in-sync")) {
                rowResult.put("equal",1);
                binds.clear();
                binds.add(0,dcRow.getTableName());
                binds.add(1,dcRow.getPkHash());
                binds.add(2, dcRow.getBatchNbr());
                dbPostgres.simpleUpdate(repoConn, "DELETE FROM dc_source WHERE table_name=? AND pk_hash=? AND batch_nbr=?", binds, true);
                dbPostgres.simpleUpdate(repoConn, "DELETE FROM dc_target WHERE table_name=? AND pk_hash=? AND batch_nbr=?", binds, true);
            } else {
                Logging.write("warning", "recheck", "Out-of-Sync:  PK = " + dcRow.getPk() + ";  Differences = " + rowResult.getJSONArray("result").toString());
            }

            binds.clear();
            binds.add(0,rowResult.getInt("equal"));
            binds.add(1,sourceRow.size());
            binds.add(2,targetRow.size());
            binds.add(3,cid);
            dbPostgres.simpleUpdate(repoConn, sqlUpdateCount, binds, true);


        } catch (Exception e) {
            Logging.write("severe", "recheck", "Error comparing source and target values:  " + e.getMessage());
        }

    }

    public static void checkRows (Connection repoConn, String sqlSource, String sqlTarget, Connection sourceConn, Connection targetConn, String sourceTable, String targetTable, ColumnMetadata ciSource, ColumnMetadata ciTarget, Integer batchNbr, Integer cid) {
        /////////////////////////////////////////////////
        // Get Column Info
        /////////////////////////////////////////////////
        ArrayList<Object> binds = new ArrayList<>();
        JSONObject result = new JSONObject();
        result.put("status","failed");
        result.put("compareStatus","failed");
        StringBuilder tableFilter;

        ////////////////////////////////////////
        // Get Out of Sync Rows
        ////////////////////////////////////////
        String sqlOutofSync = """
                        SELECT DISTINCT table_name, pk_hash, pk
                        FROM (SELECT table_name, pk_hash, pk
                            FROM dc_source
                            WHERE table_name = ?
                                  AND compare_result is not null
                                  AND compare_result != 'e'
                            UNION
                            SELECT table_name, pk_hash, pk
                            FROM dc_target
                            WHERE table_name = ?
                                  AND compare_result is not null
                                  AND compare_result != 'e') x
                        ORDER BY table_name
                       """;

        try {
            PreparedStatement stmt = repoConn.prepareStatement(sqlOutofSync);
            stmt.setObject(1, sourceTable);
            stmt.setObject(2, targetTable);
            ResultSet rs = stmt.executeQuery();


            while (rs.next()) {
                DataCompare dcRow = new DataCompare(null,null,null,null,null, 0, batchNbr);
                dcRow.setTableName(targetTable);
                dcRow.setPkHash(rs.getString("pk_hash"));
                dcRow.setPk(rs.getString("pk"));
                dcRow.setCompareResult("compare_result");
                int pkColumns = 0;
                binds.clear();
                tableFilter = new StringBuilder(" AND ");
                JSONObject pk = new JSONObject(dcRow.getPk());
                Iterator<String> keys = pk.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    if (pk.get(key) instanceof String) {
                        String value = pk.getString(key);
                        binds.add(pkColumns,value);
                    } else {
                        Integer value = pk.getInt(key);
                        binds.add(pkColumns,value);
                    }
                    tableFilter.append(key).append(" = ? AND ");
                    pkColumns++;
                }
                tableFilter = new StringBuilder(tableFilter.substring(0, tableFilter.length() - 5));
                Logging.write("info", "recheck", "Primary Key: " + pk);

                reCheck(repoConn, sourceConn, targetConn, sqlSource, sqlTarget, tableFilter.toString(), ciTarget.pkList, binds, dcRow, cid);

            }

            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", "recheck", "Error performing check of table " + targetTable + ":  " + e.getMessage());
        }

    }


}
