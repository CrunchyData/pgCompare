package com.crunchydata.controller;

import com.crunchydata.model.*;
import com.crunchydata.services.*;
import com.crunchydata.util.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.util.ArrayList;

import static com.crunchydata.util.DataUtility.*;
import static com.crunchydata.util.JsonUtility.findOne;
import static com.crunchydata.util.SQLConstants.*;
import static com.crunchydata.util.Settings.Props;

public class ColumnController {
    private static final String THREAD_NAME = "ColumnController";

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

                JSONObject joColumn = columnObject.getJSONObject(targetType);

                if (joColumn.getBoolean("primaryKey")) {
                    String pkColumn = (joColumn.getBoolean("preserveCase")) ? ShouldQuoteString(joColumn.getBoolean("preserveCase"), joColumn.getString("columnName")) : joColumn.getString("columnName").toLowerCase();
                    nbrPKColumns++;
                    pk.append(joColumn.getString("valueExpression"))
                            .append(concatOperator)
                            .append("'.'")
                            .append(concatOperator);
                    pkList.append(pkColumn).append(",");

                    if (pkJSON.isEmpty()) {
                        pkJSON.append("'{'").append(concatOperator);
                    } else {
                        pkJSON.append(concatOperator).append(" ',' ").append(concatOperator);
                    }

                    if ( joColumn.getString("dataClass").equals("char") ) {
                        pkJSON.append("'\"").append(pkColumn).append("\": \"' ")
                                .append(concatOperator).append(" ").append(pkColumn)
                                .append(" ").append(concatOperator).append(" '\"' ");
                    } else {
                        if (platform.equals("mssql")) {
                            pkJSON.append("'\"").append(pkColumn).append("\": ' ")
                                    .append(concatOperator).append(" ").append("trim(cast(")
                                    .append(pkColumn).append(" as varchar))");
                        } else {
                            pkJSON.append("'\"").append(pkColumn).append("\": ' ")
                                    .append(concatOperator).append(" ").append(pkColumn);
                        }
                    }
                } else {
                    nbrColumns++;
                    columnList.append(ShouldQuoteString(joColumn.getBoolean("preserveCase"), joColumn.getString("columnName"))).append(",");
                    column.append(useDatabaseHash
                            ? joColumn.getString("valueExpression") + concatOperator
                            : joColumn.getString("valueExpression") + " as " + joColumn.getString("columnName") + ",");
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
    public static JSONObject createColumnMap(String targetType, String platform, Connection conn, String schema, String table, JSONObject columnData) {
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
            JSONObject findColumn = findOne(columns, "alias", colExpression.getJSONObject(i).getString("columnName").toLowerCase());

            int columnPosition = findColumn.getInt("count") == 0 ? -1 : findColumn.getInt("location");

            if (columnPosition == -1) {
                columnDetail.put("alias", colExpression.getJSONObject(i).getString("columnName").toLowerCase());
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


    public static void discoverColumns(Integer pid, Connection connRepo, Connection connSource, Connection connTarget) {
        ArrayList binds = new ArrayList();

        binds.add(0, pid);
        // Clear out Previous Mappings
        dbCommon.simpleUpdate(connRepo, SQL_REPO_DCTABLECOLUMN_DELETEBYPID, binds, true);

        CachedRowSet crs;

        // Target Table Columns
        try {
            binds.clear();
            binds.add(0,pid);
            binds.add(1,"target");
            crs = dbCommon.simpleSelect(connRepo, SQL_REPO_DCTABLEMAP_SELECTBYPIDORIGIN, binds);

            while (crs.next()) {
                // Repopulate Columns
                loadColumns(pid, crs.getInt("tid"), crs.getString("schema_name"), crs.getString("table_name"), connRepo, connTarget, "target", true);
            }

            crs.close();
        } catch (Exception e) {
            Logging.write("severe",THREAD_NAME, String.format("Error retrieving columns for project %d on %s: %s",pid,"target",e.getMessage()));
            System.exit(1);
        }

        // Source Table Columns
        try {
            binds.clear();
            binds.add(0,pid);
            binds.add(1,"source");
            crs = dbCommon.simpleSelect(connRepo, SQL_REPO_DCTABLEMAP_SELECTBYPIDORIGIN, binds);

            while (crs.next()) {
                loadColumns(pid, crs.getInt("tid"), crs.getString("schema_name"), crs.getString("table_name"), connRepo, connSource, "source", true);
            }

            crs.close();
        } catch (Exception e) {
            Logging.write("severe",THREAD_NAME, String.format("Error retrieving columns for project %d on %s: %s",pid,"source",e.getMessage()));
            System.exit(1);
        }

    }

    public static Integer loadColumns(Integer pid, Integer tid, String schema, String tableName, Connection connRepo, Connection connDest, String destRole, Boolean populateDCTableColumn) {
        String destType=Props.getProperty(destRole+"-type");
        ArrayList<Object> binds = new ArrayList<>();
        Integer columnCount = 0;

        Logging.write("info", THREAD_NAME, String.format("Performing column discovery on %s for table %s", destType, tableName));

        // Get Tables based on Platform
        JSONArray columns = getTableColumns(destType,connDest,schema,tableName);

        // Get Default Case for Platform
        String nativeCase = getNativeCase(destType);

        // Populate dc_table_column and dc_table_column_map
        for (int i = 0; i < columns.length(); i++) {
            String columnName = columns.getJSONObject(i).getString("columnName");
            binds.clear();
            binds.add(0, tid);
            binds.add(1, columnName);

            Integer cid = dbCommon.simpleSelectReturnInteger(connRepo, SQL_REPO_DCTABLECOLUMN_SELECTBYTIDALIAS, binds);

            DCTableColumn dtc = new DCTableColumn();
            dtc.setTid(tid);
            dtc.setColumnAlias(columnName.toLowerCase());

            if (cid == null ) {
                if ( populateDCTableColumn ) {
                    dtc = RepoController.saveTableColumn(connRepo, dtc);
                } else {
                    Logging.write("warning", THREAD_NAME, String.format("Skpping column since no column alias found for %s on table %s.", columnName, tableName));
                }
            } else {
                dtc.setColumnID(cid);
            }

            if ( dtc.getColumnID() != null ) {
                columnCount++;
                DCTableColumnMap dctcm = new DCTableColumnMap();

                dctcm.setTid(dtc.getTid());
                dctcm.setColumnID(dtc.getColumnID());
                dctcm.setColumnOrigin(destRole);
                dctcm.setColumnName(columns.getJSONObject(i).getString("columnName"));
                dctcm.setDataType(columns.getJSONObject(i).getString("dataType"));
                dctcm.setDataClass(columns.getJSONObject(i).getString("dataClass"));
                dctcm.setDataLength(columns.getJSONObject(i).getInt("dataLength"));
                dctcm.setNumberPrecission(columns.getJSONObject(i).getInt("dataPrecision"));
                dctcm.setNumberScale(columns.getJSONObject(i).getInt("dataScale"));
                dctcm.setColumnNullable(columns.getJSONObject(i).getBoolean("nullable"));
                dctcm.setColumnPrimaryKey(columns.getJSONObject(i).getBoolean("primaryKey"));
                dctcm.setMapExpression(columns.getJSONObject(i).getString("valueExpression"));
                dctcm.setSupported(columns.getJSONObject(i).getBoolean("supported"));
                dctcm.setPreserveCase(columns.getJSONObject(i).getBoolean("preserveCase"));

                RepoController.saveTableColumnMap(connRepo, dctcm);

                Logging.write("info", THREAD_NAME, String.format("Discovered Column: %s",columnName));

            }
        }

        Logging.write("info", THREAD_NAME, String.format("Discovered %d columns for table %s",columnCount, tableName));

        return columnCount;
    }


    public static JSONArray getTableColumns (String databasePlatform, Connection conn, String schema, String tableName) {
        return switch (databasePlatform) {
            case "oracle" -> dbOracle.getColumns(conn, schema, tableName);
            case "mysql" -> dbMySQL.getColumns(conn, schema, tableName);
            case "mssql" -> dbMSSQL.getColumns(conn, schema, tableName);
            default -> dbPostgres.getColumns(conn, schema, tableName);
        };
    }

}
