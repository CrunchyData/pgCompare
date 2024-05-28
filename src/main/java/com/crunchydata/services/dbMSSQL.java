package com.crunchydata.services;

import com.crunchydata.util.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import static com.crunchydata.services.ColumnValidation.*;
import static com.crunchydata.util.Settings.Props;

public class dbMSSQL {

    // MSSQL Data Types
    //     Date/Time: datetime, smalldatetime, date, time, datetimeoffset, datetime2
    //     Numeric: bigint, int, smallint, tinyint, decimal, numeric, money, smallmoney
    //     String: char, varchar, text, nchar, nvarchar, ntext, xml
    //     Unsupported: bit, binary, varbinary, image, cursor, rowversion, hierarchyid, uniqueidentifier, sql_variant

    public static String buildLoadSQL (Boolean useDatabaseHash, String schema, String tableName, String pkColumns, String pkJSON, String columns, String tableFilter) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        String sql = "SELECT ";

        if (useDatabaseHash) {
            sql += "lower(convert(varchar, hashbytes('MD5'," + pkColumns + "),2)) pk_hash, " + pkJSON + " pk, lower(convert(varchar, hashbytes('MD5'," + columns + "),2)) column_hash FROM " + schema + "." + tableName + " WHERE 1=1 ";
        } else {
            sql += pkColumns + " pk_hash, " + pkJSON + " pk, " + columns + " FROM " + schema + "." + tableName + " WHERE 1=1 ";
        }

        if ( tableFilter != null && !tableFilter.isEmpty()) {
            sql += " AND " + tableFilter;
        }

        return sql;
    }

    public static String columnValueMapMSSQL(JSONObject column) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        String colExpression;

        if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression =   Props.getProperty("number-cast").equals("notation") ? "lower(replace(coalesce(trim(to_char(" + column.getString("columnName") + ",'E10')),' '),'E+0,'e+'))"   : "coalesce(cast(format(" + column.getString("columnName") + ",'0000000000000000000000.0000000000000000000000') as text),' ')";
        } else if ( Arrays.asList(booleanTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "case when coalesce(cast(" + column.getString("columnName") + " as varchar),'0') = 'true' then '1' else '0' end";
        } else if ( Arrays.asList(timestampTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "coalesce(format(" + column.getString("columnName") + ",MMddyyyyHHMIss'),' ')";
        } else if ( Arrays.asList(charTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "coalesce(" + column.getString("columnName") + ",' ')";
        } else {
            colExpression = column.getString("columnName");
        }

        return colExpression;

    }

    public static JSONArray getColumns (Connection conn, String schema, String table) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        ResultSet rs;
        PreparedStatement stmt;
        JSONArray columnInfo = new JSONArray();

        /////////////////////////////////////////////////
        // SQL
        /////////////////////////////////////////////////
        String sql = """
                SELECT lower(c.table_schema) owner, lower(c.table_name) table_name, lower(c.column_name) column_name, c.data_type,\s
                       coalesce(c.character_maximum_length,c.numeric_precision) data_length, coalesce(c.numeric_precision,44) data_precision, coalesce(c.numeric_scale,22) data_scale,\s
                       case when c.is_nullable='YES' then 'Y' else 'N' end nullable,
                       CASE WHEN pkc.column_name IS NULL THEN 'N' ELSE 'Y' END pk
                FROM information_schema.columns c
                     LEFT OUTER JOIN (SELECT tc.table_schema, tc.table_name, kcu.column_name, kcu.ORDINAL_POSITION column_position
                	  				  FROM information_schema.table_constraints tc
                					  	   INNER JOIN information_schema.key_column_usage kcu
                								ON tc.constraint_catalog = kcu.constraint_catalog
                									AND tc.constraint_schema = kcu.constraint_schema
                									AND tc.constraint_name = kcu.constraint_name
                									AND tc.table_name = kcu.table_name
                					WHERE tc.constraint_type='PRIMARY KEY')  pkc ON (c.table_schema=pkc.table_schema AND c.table_name=pkc.table_name AND c.column_name=pkc.column_name)
                WHERE c.table_schema=?
                      AND c.table_name=?
                ORDER BY c.table_schema, c.table_name, c.column_name
                """;

        try {
            stmt = conn.prepareStatement(sql);
            stmt.setObject(1, schema);
            stmt.setObject(2,table);
            rs = stmt.executeQuery();
            while (rs.next()) {
                JSONObject column = new JSONObject();
                if ( Arrays.asList(unsupportedDataTypes).contains(rs.getString("data_type").toLowerCase()) ) {
                    Logging.write("severe", "mssql-service", "Unsupported data type (" + rs.getString("data_type") + ")");
                    column.put("supported",false);
                } else {
                    column.put("supported",true);
                }
                column.put("columnName",rs.getString("column_name"));
                column.put("dataType",rs.getString("data_type"));
                column.put("dataLength",rs.getInt("data_length"));
                column.put("dataPrecision",rs.getInt("data_precision"));
                column.put("dataScale",rs.getInt("data_scale"));
                column.put("nullable",rs.getString("nullable").equals("Y"));
                column.put("primaryKey",rs.getString("pk").equals("Y"));
                column.put("valueExpression", columnValueMapMSSQL(column));
                column.put("dataClass", getDataClass(rs.getString("data_type").toLowerCase()));

                columnInfo.put(column);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", "mssql-service", "Error retrieving columns for table " + schema + "." + table + ":  " + e.getMessage());
        }
        return columnInfo;
    }

    public static Connection getConnection(Properties connectionProperties, String destType) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        Connection conn;
        conn = null;
        String url = "jdbc:sqlserver://"+connectionProperties.getProperty(destType+"-host")+":"+connectionProperties.getProperty(destType+"-port")+";databaseName="+connectionProperties.getProperty(destType+"-dbname")+";encrypt="+(connectionProperties.getProperty(destType+"-sslmode").equals("disable") ? "false" : "true");
        Properties dbProps = new Properties();

        dbProps.setProperty("user",connectionProperties.getProperty(destType+"-user"));
        dbProps.setProperty("password",connectionProperties.getProperty(destType+"-password"));

        try {
            conn = DriverManager.getConnection(url,dbProps);
        } catch (Exception e) {
            Logging.write("severe", "mssql-service", "Error connecting to MSSQL " + e.getMessage());
        }

        return conn;

    }

    public static String getVersion (Connection conn) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        String dbVersion = null;
        ArrayList<Object> binds = new ArrayList<>();

        try {
            CachedRowSet crsVersion = dbMSSQL.simpleSelect(conn, "select version()", binds);

            if (crsVersion.next()) {
                dbVersion = crsVersion.getString("version");
            }

            crsVersion.close();

        } catch (Exception e) {
            Logging.write("info", "mssql-service", "Could not retrieve MSSQL version " + e.getMessage());
        }

        return dbVersion;
    }

    public static CachedRowSet simpleSelect(Connection conn, String sql, ArrayList<Object> binds) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        ResultSet rs;
        PreparedStatement stmt;
        CachedRowSet crs = null;

        try {
            crs = RowSetProvider.newFactory().createCachedRowSet();
            stmt = conn.prepareStatement(sql);
            stmt.setFetchSize(2000);
            for (int counter = 0; counter < binds.size(); counter++) {
                stmt.setObject(counter+1, binds.get(counter));
            }
            rs = stmt.executeQuery();
            crs.populate(rs);
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", "mssql-service", "Error executing simple select (" + sql + "): " + e.getMessage());
        }
        return crs;
    }

    public static Integer simpleUpdate(Connection conn, String sql, ArrayList<Object> binds, boolean commit) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        int cnt;
        PreparedStatement stmt;

        try {
            stmt = conn.prepareStatement(sql);
            for (int counter = 0; counter < binds.size(); counter++) {
                stmt.setObject(counter+1, binds.get(counter));
            }
            cnt = stmt.executeUpdate();
            stmt.close();
            if (commit) {
                conn.commit();
            }
        } catch (Exception e) {
            Logging.write("severe", "mssql-service", "Error executing simple update (" + sql + "):  " + e.getMessage());
            try { conn.rollback(); } catch (Exception ee) {
                // do nothing
            }
            cnt = -1;
        }
        return cnt;
    }
}
