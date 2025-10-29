package com.crunchydata.util;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.crunchydata.config.Settings.Props;
import static com.crunchydata.config.Settings.validPropertyValues;

/**
 * Utility class for preflight validation operations.
 * Provides methods to validate application settings and database configurations before execution.
 *
 * <p>This class handles validation of properties, database settings, and action-specific
 * requirements to ensure the application can run successfully.</p>
 *
 * @author Brian Pace
 */
public class ValidationUtils {

    private static final String THREAD_NAME = "preflight-util";
    
    // Constants for better maintainability
    private static final String COPY_TABLE_ACTION = "copy-table";
    private static final String TABLE_PROPERTY = "table";
    private static final String IS_CHECK_PROPERTY = "isCheck";
    private static final String TRUE_VALUE = "true";
    private static final String COLUMN_HASH_METHOD_PROPERTY = "column-hash-method";
    private static final String DATABASE_HASH_METHOD = "database";
    private static final String FLOAT_SCALE = "float-scale";
    private static final String HYBRID_HASH_METHOD = "hybrid";
    private static final String NUMBER_CAST_PROPERTY = "number-cast";
    private static final String NOTATION_CAST = "notation";
    private static final String STANDARD_CAST = "standard";
    private static final String STANDARD_NUMBER_FORMAT_PROPERTY = "standard-number-format";
    private static final String DB2_PRECISION_FORMAT = "0000000000000000000000000000000.0000000000000000000000000000000";
    private static final String SNOWFLAKE_PRECISION_FORMAT = "999999999999999999990.0000000000000000";

    /**
     * Performs all preflight validation checks.
     *
     * @param action The action being performed
     * @return true if all validations pass, false otherwise
     */
    public static boolean all(String action) {
        // Properties Preflight
        if (!validateProperties()) {
            LoggingUtils.write("severe", THREAD_NAME, "Invalid properties");
            return false;
        }

        // Action Preflights
        if (COPY_TABLE_ACTION.equals(action)) {
            if (Props.getProperty(TABLE_PROPERTY).length() > 0) {
                LoggingUtils.write("severe", THREAD_NAME, "Must specify a table alias to copy using --table option");
                return false;
            }
        }

        // Database Preflight
        ValidationUtils.database(Props, "source");
        ValidationUtils.database(Props, "target");

        return true;
    }

    /**
     * Performs database-specific preflight validation and configuration adjustments.
     *
     * @param Props Properties object containing configuration
     * @param targetType The target type (source/target)
     */
    public static void database(Properties Props, String targetType) {
        LoggingUtils.write("info", THREAD_NAME, String.format("Performing Preflight checks for %s", targetType));

        String databaseType = Props.getProperty(targetType + "-type");

        // Handle check mode hash method adjustment
        if (TRUE_VALUE.equals(Props.getProperty(IS_CHECK_PROPERTY)) && DATABASE_HASH_METHOD.equals(Props.getProperty(COLUMN_HASH_METHOD_PROPERTY))) {
            LoggingUtils.write("info", THREAD_NAME, "Switching column hash method to hybrid for check");
            Props.setProperty(COLUMN_HASH_METHOD_PROPERTY, HYBRID_HASH_METHOD);
        }

        switch (databaseType) {
            case "db2":
                handleDB2Configuration(Props);
                break;
            case "snowflake":
                handleSnowflakeConfiguration(Props);
                break;
            case "mariadb", "mysql", "oracle", "postgres":
                // No restrictions for these databases
                break;
            case "mssql":
                handleMSSQLConfiguration(Props);
                break;
        }
    }
    
    /**
     * Handles DB2-specific configuration adjustments.
     *
     * @param Props Properties object to modify
     */
    private static void handleDB2Configuration(Properties Props) {
        // Number Cast must be standard for DB2
        if (NOTATION_CAST.equals(Props.getProperty(NUMBER_CAST_PROPERTY))) {
            LoggingUtils.write("warning", THREAD_NAME, "Switching number-cast to standard as required for DB2");
            Props.setProperty(NUMBER_CAST_PROPERTY, STANDARD_CAST);
        }

        // Database side hash is not supported for DB2
        if (DATABASE_HASH_METHOD.equals(Props.getProperty(COLUMN_HASH_METHOD_PROPERTY))) {
            LoggingUtils.write("warning", THREAD_NAME, "Switching column hash method to hybrid as required for DB2");
            Props.setProperty(COLUMN_HASH_METHOD_PROPERTY, HYBRID_HASH_METHOD);
        }

        // Precision must be limited to 38
        if ( !Props.getProperty(STANDARD_NUMBER_FORMAT_PROPERTY).equals(DB2_PRECISION_FORMAT) ) {
            LoggingUtils.write("warning", THREAD_NAME, "Switching standard-number-format to precision of 31 as required for DB2");
            Props.setProperty(STANDARD_NUMBER_FORMAT_PROPERTY, DB2_PRECISION_FORMAT);
        }
    }

    private static void handleSnowflakeConfiguration(Properties Props) {
        // Number Cast must be standard for Snowflake
        if (NOTATION_CAST.equals(Props.getProperty(NUMBER_CAST_PROPERTY))) {
            LoggingUtils.write("warning", THREAD_NAME, "Switching number-cast to standard");
            Props.setProperty(NUMBER_CAST_PROPERTY, STANDARD_CAST);
        }

        // Precision must be limited to 38
        if ( !Props.getProperty(STANDARD_NUMBER_FORMAT_PROPERTY).equals(SNOWFLAKE_PRECISION_FORMAT) ) {
            LoggingUtils.write("warning", THREAD_NAME, "Switching standard-number-format to precision of 38 as required for Snowflake");
            Props.setProperty(STANDARD_NUMBER_FORMAT_PROPERTY, SNOWFLAKE_PRECISION_FORMAT);
        }

        // Set scale to 1
        if ( !Props.getProperty(FLOAT_SCALE).equals("1")) {
            LoggingUtils.write("warning", THREAD_NAME, "Switching float-scale to 1 as required for Snowflake");
            Props.setProperty(FLOAT_SCALE, "1");
        }

    }

    /**
     * Handles MSSQL-specific configuration adjustments.
     *
     * @param Props Properties object to modify
     */
    private static void handleMSSQLConfiguration(Properties Props) {
        // Database side hash is not supported for MSSQL
        if (DATABASE_HASH_METHOD.equals(Props.getProperty(COLUMN_HASH_METHOD_PROPERTY))) {
            LoggingUtils.write("warning", THREAD_NAME, "Switching column hash method to hybrid as required for MSSQL");
            Props.setProperty(COLUMN_HASH_METHOD_PROPERTY, HYBRID_HASH_METHOD);
        }
    }

    /**
     * Validates all properties against their valid values.
     *
     * @return true if all properties are valid, false otherwise
     */
    public static boolean validateProperties() {
        for (Map.Entry<String, Set<String>> entry : validPropertyValues.entrySet()) {
            String propertyName = entry.getKey();
            Set<String> validValues = entry.getValue();

            if (!validValues.contains(Props.getProperty(propertyName))) {
                LoggingUtils.write("severe", "Settings", String.format("Property %s has an invalid value. Valid values are: %s", propertyName, validValues));
                return false;
            }
        }

        return true;
    }

}
