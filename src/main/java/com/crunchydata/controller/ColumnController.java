package com.crunchydata.controller;

import com.crunchydata.models.*;
import com.crunchydata.services.*;
import com.crunchydata.util.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Properties;

import static com.crunchydata.util.ColumnUtility.getColumns;
import static com.crunchydata.util.DataUtility.*;
import static com.crunchydata.util.SQLConstantsRepo.*;

@SuppressWarnings("ALL")
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
        Logging.write("info", THREAD_NAME, String.format("(%s) Building column expressions for %s.%s",targetType, schema,table));

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
                        pkJSON.append("'\"").append(pkColumn.replace("\"","")).append("\": \"' ")
                                .append(concatOperator).append(" ").append(pkColumn)
                                .append(" ").append(concatOperator).append(" '\"' ");
                    } else {
                        if (platform.equals("mssql")) {
                            pkJSON.append("'\"").append(pkColumn.replace("\"","")).append("\": ' ")
                                    .append(concatOperator).append(" ").append("trim(cast(")
                                    .append(pkColumn).append(" as varchar))");
                        } else {
                            pkJSON.append("'\"").append(pkColumn.replace("\"","")).append("\": ' ")
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
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", THREAD_NAME, String.format("Error while parsing column list at line %s:  %s", stackTrace[0].getLineNumber(), e.getMessage()));
        }

        // Using the concat operator causes issues for mariadb.  Have to convert from using operator (||)
        // to using concat function.
        if ( platform.equals("mariadb")) {
            pkJSON = new StringBuilder("concat(" + pkJSON.toString().replace("||",",") + ")");
        }

        return new ColumnMetadata(columnList.toString(), nbrColumns, nbrPKColumns, column.toString(), pk.toString(), pkList.toString(), pkJSON.toString());

    }


    public static void discoverColumns(Properties Props, Integer pid, Connection connRepo, Connection connSource, Connection connTarget) {
        ArrayList<Object> binds = new ArrayList<>();

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
                loadColumns(Props, pid, crs.getInt("tid"), crs.getString("schema_name"), crs.getString("table_name"), connRepo, connTarget, "target", true);
            }

            crs.close();
        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe",THREAD_NAME, String.format("Error retrieving columns for project %d on %s at line %s: %s",pid,"target",stackTrace[0].getLineNumber(), e.getMessage()));
            System.exit(1);
        }

        // Source Table Columns
        try {
            binds.clear();
            binds.add(0,pid);
            binds.add(1,"source");
            crs = dbCommon.simpleSelect(connRepo, SQL_REPO_DCTABLEMAP_SELECTBYPIDORIGIN, binds);

            while (crs.next()) {
                loadColumns(Props, pid, crs.getInt("tid"), crs.getString("schema_name"), crs.getString("table_name"), connRepo, connSource, "source", true);
            }

            crs.close();
        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe",THREAD_NAME, String.format("Error retrieving columns for project %d on %s at line %s: %s",pid,"source", stackTrace[0].getLineNumber(), e.getMessage()));
            System.exit(1);
        }

    }

    public static void loadColumns(Properties Props, Integer pid, Integer tid, String schema, String tableName, Connection connRepo, Connection connDest, String destRole, Boolean populateDCTableColumn) {
        String destType=Props.getProperty(destRole+"-type");
        ArrayList<Object> binds = new ArrayList<>();
        Integer columnCount = 0;

        Logging.write("info", THREAD_NAME, String.format("(%s) Performing column discovery on %s for table %s", destRole, destType, tableName));

        // Get Tables based on Platform
        JSONArray columns = getColumns(Props, connDest,schema,tableName, destRole);

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
                    Logging.write("warning", THREAD_NAME, String.format("Skipping column since no column alias found for %s on table %s.", columnName, tableName));
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
                dctcm.setNumberPrecision(columns.getJSONObject(i).getInt("dataPrecision"));
                dctcm.setNumberScale(columns.getJSONObject(i).getInt("dataScale"));
                dctcm.setColumnNullable(columns.getJSONObject(i).getBoolean("nullable"));
                dctcm.setColumnPrimaryKey(columns.getJSONObject(i).getBoolean("primaryKey"));
                dctcm.setMapExpression(columns.getJSONObject(i).getString("valueExpression"));
                dctcm.setSupported(columns.getJSONObject(i).getBoolean("supported"));
                dctcm.setPreserveCase(columns.getJSONObject(i).getBoolean("preserveCase"));

                RepoController.saveTableColumnMap(connRepo, dctcm);

                Logging.write("info", THREAD_NAME, String.format("(%s) Discovered Column: %s",destRole,columnName));

            }
        }

        Logging.write("info", THREAD_NAME, String.format("(%s) Discovered %d columns for table %s",destRole, columnCount, tableName));

    }

}
