package com.crunchydata.util;

import java.util.Properties;

public class Preflight {

    private static final String THREAD_NAME = "preflight-util";

    /**
     * Preflight method to validate settings
     *
     */
    public static void database (Properties Props, String targetType) {
        Logging.write("info",THREAD_NAME,String.format("Performing Preflight checks for %s",targetType));

        String databaseType = Props.getProperty(targetType + "-type");

        if (Props.getProperty("isCheck").equals("true") && Props.getProperty("column-hash-method").equals("database")) {
            Logging.write("info",THREAD_NAME,"Switching column hash method to hybrid for check");
            Props.setProperty("column-hash-method","hybrid");
        }

        switch (databaseType) {
            case "db2":
                // Number Cast must be standard
                if (Props.getProperty("number-cast").equals("notation")) {
                    Logging.write("warning",THREAD_NAME,"Switching number-cast to standard and standard-number-format to precision of 31 as required for DB2");
                    Props.setProperty("number-cast","standard");
                    Props.setProperty("standard-number-format","0000000000000000000000000000000.0000000000000000000000000000000");
                }

                // Database side hash is not supported for DB2
                if ("database".equals(Props.getProperty("column-hash-method")) ) {
                    Logging.write("warning",THREAD_NAME,"Switching column hash method to hybrid as required for DB2");
                    Props.setProperty("column-hash-method","hybrid");
                }

                break;

            case "mariadb", "mysql", "oracle", "postgres":
                // No restrictions
                break;

            case "mssql":
                // Database side hash is not supported for MSSQL
                if ("database".equals(Props.getProperty("column-hash-method"))) {
                    Logging.write("warning",THREAD_NAME,"Switching column hash method to hybrid as required for MSSQL");
                    Props.setProperty("column-hash-method","hybrid");
                }
                break;

        }

    }

}
