package com.crunchydata.services;

import com.crunchydata.util.Logging;

import static com.crunchydata.util.Settings.Props;

public class preflight {

    private static final String THREAD_NAME = "preflight";

    /**
     * Preflight method to validate settings used for DB2.
     *
     */
    public static void database (String targetType) {
        Logging.write("info",THREAD_NAME,String.format("Performing preflight checks for %s",targetType));

        String databaseType = Props.getProperty(targetType + "-type");

        switch (databaseType) {
            case "db2":
                // Number Cast must be standard
                if (Props.getProperty("number-cast").equals("notation")) {
                    Logging.write("warning",THREAD_NAME,"Switching number-cast to standard and standard-number-format to precision of 31 as required for DB2");
                    Props.setProperty("number-cast","standard");
                    Props.setProperty("standard-number-format","0000000000000000000000000000000.0000000000000000000000000000000");
                }

                // Database side hash is not supported for DB2
                if (Props.getProperty(targetType + "-database-hash").equals("true")) {
                    Logging.write("warning",THREAD_NAME,"Switching database-hash to false as required for DB2");
                    Props.setProperty(targetType + "-database-hash","false");
                }

                break;

            case "mysql":
                // No restrictions
                break;

            case "mssql":
                // Database side hash is not supported for MSSQL
                if (Props.getProperty(targetType + "-database-hash").equals("true")) {
                    Logging.write("warning",THREAD_NAME,"Switching database-hash to false as required for MSSQL");
                    Props.setProperty(targetType + "-database-hash","false");
                }
                break;

            case "oracle":
                // No restrictions
                break;

            case "postgres":
                // No restrictions
                break;
        }

    }



}
