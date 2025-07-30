package com.crunchydata.controller;

import com.crunchydata.models.*;
import com.crunchydata.services.*;
import com.crunchydata.util.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.crunchydata.services.DatabaseService.getConcatOperator;
import static com.crunchydata.services.DatabaseService.getQuoteChar;
import static com.crunchydata.util.CastUtility.cast;
import static com.crunchydata.util.CastUtility.castRaw;
import static com.crunchydata.util.ColumnUtility.getColumns;
import static com.crunchydata.util.DataUtility.*;
import static com.crunchydata.util.JsonUtility.buildJsonExpression;
import static com.crunchydata.util.SQLConstantsRepo.*;
import static com.crunchydata.util.Settings.Props;

/**
 * ColumnController class that collects column metadata.
 *
 * @author Brian Pace
 */
public class ColumnController {
    private static final String THREAD_NAME = "column-ctrl";

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

        String concatOperator = getConcatOperator(platform);

        int nbrColumns = 0;
        int nbrPKColumns = 0;

        List<String> pkList = new ArrayList<>();
        List<String> columnList = new ArrayList<>();
        List<String> columnExpressionList = new ArrayList<>();
        List<String> pkHash = new ArrayList<>();
        List<String> pkJSON = new ArrayList<>();

        String quoteChar = getQuoteChar(platform);

        // Gather column metadata from the columnMap json and save in the ColumnMetadata class
        try {
            JSONArray columnsArray = columnMap.getJSONArray("columns");
            for (int i = 0; i < columnsArray.length(); i++) {
                JSONObject columnObject = columnsArray.getJSONObject(i);

                if (columnObject.getBoolean("enabled")) {
                    JSONObject joColumn = columnObject.getJSONObject(targetType);

                    String columnName = ShouldQuoteString(
                            joColumn.getBoolean("preserveCase"),
                            joColumn.getString("columnName"),
                            quoteChar
                    );

                    String dataType = joColumn.getString("dataType").toLowerCase();
                    String dataClass = joColumn.getString("dataClass");
                    String columnHashMethod = Props.getProperty("column-hash-method");

                    // If map_expression is not overridden, generate default expression
                    if ( joColumn.isNull("valueExpression") || joColumn.getString("valueExpression").isEmpty() ) {
                        joColumn.put("valueExpression",  "raw".equals(columnHashMethod)
                                ? castRaw(dataType, columnName, platform)
                                : cast(dataType, columnName, platform, joColumn)
                        );
                    } else {
                        Logging.write("info", THREAD_NAME, String.format("(%s) Using custom column expression for column %s: %s", targetType, columnObject.getString("columnAlias"),joColumn.getString("valueExpression")));
                    }

                    Logging.write("debug", THREAD_NAME, String.format("(%s) Mapping expression for column %s: %s", targetType, columnObject.getString("columnAlias"), joColumn.getString("valueExpression")));

                    // Identify if column is primary key and save primary keys to pk string
                    if (joColumn.getBoolean("primaryKey")) {
                        String pkColumn = (joColumn.getBoolean("preserveCase")) ? ShouldQuoteString(joColumn.getBoolean("preserveCase"), joColumn.getString("columnName"), getQuoteChar(platform)) : joColumn.getString("columnName").toLowerCase();
                        nbrPKColumns++;

                        pkHash.add(joColumn.getString("valueExpression"));

                        pkList.add(pkColumn);

                        pkJSON.add(buildJsonExpression(platform, pkColumn, dataClass, concatOperator));

                    } else {
                        // Process non-primary key columns
                        nbrColumns++;
                        columnList.add(ShouldQuoteString(joColumn.getBoolean("preserveCase"), joColumn.getString("columnName"), getQuoteChar(platform)));
                        columnExpressionList.add(useDatabaseHash
                                ? joColumn.getString("valueExpression")
                                : joColumn.getString("valueExpression") + " as " + joColumn.getString("columnName").toLowerCase());
                    }

                } else {
                    Logging.write("warning", THREAD_NAME, String.format("Skipping disabled column:  %s", columnObject.getString("columnAlias")));
                }


            }

            if (columnList.isEmpty()) {
                columnExpressionList.add((useDatabaseHash) ? "'0'" : " '0' as c1");
                nbrColumns = 1;
            }

        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", THREAD_NAME, String.format("Error while parsing column list at line %s:  %s", stackTrace[0].getLineNumber(), e.getMessage()));
        }

        String finalPkList = String.join(",", pkList);
        String finalColumnList = String.join(",", columnList);
        String finalPkHash = buildFinalPkHash(pkHash, platform, concatOperator);
        String finalPkJson = buildFinalPkJson(pkJSON, platform, concatOperator);
        String finalColumnExpressionList = buildFinalColumnExpr(columnExpressionList, concatOperator, useDatabaseHash);

        return new ColumnMetadata(finalColumnList, nbrColumns, nbrPKColumns, finalColumnExpressionList, finalPkHash, finalPkList, finalPkJson);

    }


    /**
     * Create final expression for injection into SQL statement.
     *
     * @param columns           Column expressions
     * @param concatOperator    Operator to use for concatenation
     * @param useDatabaseHash   Boolean indicating if the hash is performed on the database or not
     * @return                  Expression for SQL statement
     */
    private static String buildFinalColumnExpr(List<String> columns, String concatOperator, boolean useDatabaseHash) {
        if (columns.isEmpty()) {
            return useDatabaseHash ? "'0'" : "'0' c1";
        }

        return String.join(useDatabaseHash ? concatOperator : ",", columns);
    }

    /**
     *
     * @param pkJson            String to construct JSON object in SQL
     * @param platform          Database platform
     * @param concatOperator    Operator to use for concatenation
     * @return                  Returns string for insertion into SQL
     */
    private static String buildFinalPkJson(List<String> pkJson, String platform, String concatOperator) {
        if (pkJson.isEmpty()) return "";

        String joined = String.join(concatOperator + " ',' " + concatOperator, pkJson);
        String fullExpr = "'{'" + concatOperator + joined + concatOperator + "'}'";

        // Using the concat operator causes issues for mariadb.  Have to convert from using operator (||)
        // to using concat function.
        if (platform.equals("mariadb")) {
            return "concat(" + fullExpr.replace("||", ",") + ")";
        }

        return fullExpr;
    }

    /**
     *
     * @param pkHash            Expression to construct hash of primary key columns
     * @param platform          Database platform
     * @param concatOperator    Operator to use for concatenation
     * @return                  Returns string for insertion into SQL
     */
    private static String buildFinalPkHash(List<String> pkHash, String platform, String concatOperator) {
        if (pkHash.isEmpty()) return "";

        String joined = String.join(
                platform.equals("db2") || platform.equals("oracle") ? concatOperator + "'.'" + concatOperator : ",'.',",
                pkHash);

        if (platform.equals("postgres") || platform.equals("mariadb") || platform.equals("mssql") || platform.equals("mysql")) {
            return pkHash.size() > 1 ? "concat(" + joined + ")" : joined;
        }

        return joined;
    }

    /**
     * Prepare to discover columns from source and target.
     *
     * @param Props            Properties for application settings
     * @param pid              Project id
     * @param table            Table to discover columns for
     * @param connRepo         Connection to repository
     * @param connSource       Connection to source database
     * @param connTarget       Connection to target database
     */
    public static void discoverColumns(Properties Props, Integer pid, String table, Connection connRepo, Connection connSource, Connection connTarget) {
        ArrayList<Object> binds = new ArrayList<>();

        binds.add(0, pid);
        if (! table.isEmpty()) {
            binds.add(1,table);
        }

        String sql = (table.isEmpty()) ? SQL_REPO_DCTABLECOLUMN_DELETEBYPID : SQL_REPO_DCTABLECOLUMN_DELETEBYPIDTABLE;

        // Clear out Previous Mappings
        SQLService.simpleUpdate(connRepo, sql, binds, true);

        CachedRowSet crs;

        // Target Table Columns
        sql = (table.isEmpty()) ? SQL_REPO_DCTABLEMAP_SELECTBYPIDORIGIN : SQL_REPO_DCTABLEMAP_SELECTBYPIDORIGINTABLE;

        try {
            binds.clear();
            binds.add(0,pid);
            binds.add(1,"target");
            if (! table.isEmpty()) {
                binds.add(2,table);
            }

            crs = SQLService.simpleSelect(connRepo, sql, binds);

            while (crs.next()) {
                // Repopulate Columns
                loadColumns(Props, crs.getInt("tid"), crs.getString("schema_name"), crs.getString("table_name"), connRepo, connTarget, "target", true);
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
            if (! table.isEmpty()) {
                binds.add(2,table);
            }

            crs = SQLService.simpleSelect(connRepo, sql, binds);

            while (crs.next()) {
                loadColumns(Props, crs.getInt("tid"), crs.getString("schema_name"), crs.getString("table_name"), connRepo, connSource, "source", true);
            }

            crs.close();
        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe",THREAD_NAME, String.format("Error retrieving columns for project %d on %s at line %s: %s",pid,"source", stackTrace[0].getLineNumber(), e.getMessage()));
            System.exit(1);
        }

    }

    /**
     * Perform column discovery
     *
     * @param Props            Properties for application settings
     * @param tid              Table id
     * @param schema           Schema name
     * @param tableName        Table name
     * @param connRepo         Connection to repository
     * @param connDest         Connection to source or target database
     * @param destRole         Destination role (source or target)
     * @param populateDCTableColumn  Whether to populate the DCTableColumn with column alias
     */
    public static void loadColumns(Properties Props, Integer tid, String schema, String tableName, Connection connRepo, Connection connDest, String destRole, Boolean populateDCTableColumn) {
        String destType=Props.getProperty(destRole+"-type");
        ArrayList<Object> binds = new ArrayList<>();
        Integer columnCount = 0;

        Logging.write("info", THREAD_NAME, String.format("(%s) Performing column discovery on %s for table %s", destRole, destType, tableName));

        // Get Tables based on Platform
        JSONArray columns = getColumns(Props, connDest,schema,tableName, destRole);

        // Populate dc_table_column and dc_table_column_map
        for (int i = 0; i < columns.length(); i++) {
            String columnName = columns.getJSONObject(i).getString("columnName");
            binds.clear();
            binds.add(0, tid);
            binds.add(1, columnName);

            Integer cid = SQLService.simpleSelectReturnInteger(connRepo, SQL_REPO_DCTABLECOLUMN_SELECTBYTIDALIAS, binds);

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
                dctcm.setSupported(columns.getJSONObject(i).getBoolean("supported"));
                dctcm.setPreserveCase(columns.getJSONObject(i).getBoolean("preserveCase"));

                RepoController.saveTableColumnMap(connRepo, dctcm);

                Logging.write("info", THREAD_NAME, String.format("(%s) Discovered Column: %s",destRole,columnName));

            }
        }

        Logging.write("info", THREAD_NAME, String.format("(%s) Discovered %d columns for table %s",destRole, columnCount, tableName));

    }

}
