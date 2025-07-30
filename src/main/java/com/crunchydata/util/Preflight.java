package com.crunchydata.util;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.crunchydata.util.Settings.Props;
import static com.crunchydata.util.Settings.validPropertyValues;

public class Preflight {

    private static final String THREAD_NAME = "preflight-util";

    public static boolean all (String action) {

        // Properties Preflight
        if ( !validateProperties()) {
            Logging.write("severe", THREAD_NAME, "Invalid properties");
            return false;
        }

        // Action Preflights
        if ( action.equals("copy-table") ) {
            if (Props.getProperty("table").length() > 0) {
                Logging.write("severe", THREAD_NAME, "Must specify a table alias to copy using --table option");
                return false;
            }
        }

        // Database Preflight
        Preflight.database(Props,"source");
        Preflight.database(Props,"target");

        return true;
    }

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

    public static boolean validateProperties () {
        for (Map.Entry<String, Set<String>> entry : validPropertyValues.entrySet() ) {
            String propertyName = entry.getKey();
            Set<String> validValues = entry.getValue();

            if (! validValues.contains(Props.getProperty(propertyName))) {
                Logging.write("severe","Settings", String.format("Property %s has an invalid value.  Valid values are: %s", propertyName, validValues.toString()));
                return false;
            }

        }

        return true;
    }

}
