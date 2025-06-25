package com.crunchydata.services;

import com.crunchydata.util.Logging;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Properties;

import static com.crunchydata.util.Settings.Props;

public class dbConnection {

    private static final String THREAD_NAME = "connection";

    /**
     * Close database connection.
     *
     * @param conn Database connection
     */
    public static void closeDatabaseConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                Logging.write("warning", THREAD_NAME, "Error closing connection: " + e.getMessage());
            }
        }
    }

    /**
     * Establishes a connection to an DB2 database using the provided connection properties.
     *
     * @param destType             Type of destination (e.g., source, target).
     * @return Connection object to DB2 database.
     */
    public static Connection getConnection(String platform, String destType) {
        Connection conn = null;
        String url =  switch (platform) {
            case "oracle" -> "jdbc:oracle:thin:@//"+Props.getProperty(destType+"-host")+":"+Props.getProperty(destType+"-port")+"/"+Props.getProperty(destType+"-dbname");
            case "mariadb" -> "jdbc:mariadb://"+Props.getProperty(destType+"-host")+":"+Props.getProperty(destType+"-port")+"/"+Props.getProperty(destType+"-dbname")+"?allowPublicKeyRetrieval=true&useSSL="+(Props.getProperty(destType+"-sslmode").equals("disable") ? "false" : "true");
            case "mysql" -> "jdbc:mysql://"+Props.getProperty(destType+"-host")+":"+Props.getProperty(destType+"-port")+"/"+Props.getProperty(destType+"-dbname")+"?allowPublicKeyRetrieval=true&useSSL="+(Props.getProperty(destType+"-sslmode").equals("disable") ? "false" : "true");
            case "mssql" -> "jdbc:sqlserver://"+Props.getProperty(destType+"-host")+":"+Props.getProperty(destType+"-port")+";databaseName="+Props.getProperty(destType+"-dbname")+";encrypt="+(Props.getProperty(destType+"-sslmode").equals("disable") ? "false" : "true");
            case "db2" ->  "jdbc:db2://" + Props.getProperty(destType + "-host") + ":" + Props.getProperty(destType + "-port") + "/" + Props.getProperty(destType + "-dbname");
            default -> "jdbc:postgresql://"+Props.getProperty(destType+"-host")+":"+Props.getProperty(destType+"-port")+"/"+Props.getProperty(destType+"-dbname")+"?sslmode="+Props.getProperty(destType+"-sslmode");
        };

        Properties dbProps = new Properties();

        dbProps.setProperty("user", Props.getProperty(destType + "-user"));
        dbProps.setProperty("password", Props.getProperty(destType + "-password"));

        if (platform.equals("postgres")) {
            dbProps.setProperty("options","-c search_path="+Props.getProperty(destType+"-schema")+",public,pg_catalog");
            dbProps.setProperty("reWriteBatchedInserts", "true");
            dbProps.setProperty("preparedStatementCacheQueries", "5");
            dbProps.setProperty("ApplicationName", "pgcompare");
            dbProps.setProperty("synchronous_commit", "off");
        }

        try {
            // Connect to database
            conn = DriverManager.getConnection(url, dbProps);

            // Set Platform specific options
            switch(platform) {
                case "db2":
                    conn.setAutoCommit(true);
                    break;
                case "mariadb":
                case "mysql":
                    dbCommon.simpleUpdate(conn,"set session sql_mode='ANSI'", new ArrayList<>(), false);
                    break;
                case "postgres":
                    conn.setAutoCommit(false);
                    break;
            }

        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("(%s) Error connecting to %s using %s: %s", destType, platform, url, e.getMessage()));
        }

        return conn;

    }
}