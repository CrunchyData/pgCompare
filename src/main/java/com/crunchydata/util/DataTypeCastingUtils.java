package com.crunchydata.util;

import org.json.JSONObject;

import static com.crunchydata.util.ColumnMetadataUtils.*;
import static com.crunchydata.config.Settings.Props;

/**
 * Utility class for handling database column casting operations across different database platforms.
 * This class provides methods to cast various data types to standardized formats for comparison.
 *
 * @author Brian Pace
 */
public class DataTypeCastingUtils {
    
    // Constants for better maintainability
    private static final String NOTATION_CAST = "notation";
    private static final String RAW_HASH_METHOD = "raw";
    private static final String EMPTY_STRING = " ";
    private static final String UTC_TIMEZONE = "UTC";
    private static final String TIMEZONE_INDICATOR = "time zone";
    private static final String TZ_INDICATOR = "tz";
    private static final String TEXT_DATA_TYPE = "text";
    private static final String LOB_DATA_TYPE = "lob";

    /**
     * Main casting method that determines the appropriate cast function based on data type.
     *
     * @param dataType The database data type
     * @param columnName The column name to cast
     * @param platform The database platform
     * @param column JSON object containing column metadata
     * @return SQL expression for casting the column
     */
    public static String cast(String dataType, String columnName, String platform, JSONObject column) {
        if (BOOLEAN_TYPES.contains(dataType)) {
            return castBoolean(dataType, columnName, platform);
        }
        if (BINARY_TYPES.contains(dataType)) {
            return castBinary(dataType, columnName, platform);
        }
        if (NUMERIC_TYPES.contains(dataType)) {
            return castNumber(dataType, columnName, platform);
        }
        if (STRING_TYPES.contains(dataType)) {
            return castString(dataType, columnName, platform, column);
        }
        if (TIMESTAMP_TYPES.contains(dataType)) {
            return castTimestamp(dataType, columnName, platform);
        }
        return castRaw(dataType, columnName, platform);
    }


    /**
     * Casts binary data types to standardized format for comparison.
     *
     * @param dataType The database data type
     * @param columnName The column name to cast
     * @param platform The database platform
     * @return SQL expression for casting binary data
     */
    public static String castBinary(String dataType, String columnName, String platform) {
        return switch (platform) {
            // Snowflake TODO: Add support for binary types.
            // MSSQL does not have a binary type.
            // Postgres and MySQL use the same function for binary types.
            case "db2" ->
                    String.format("case when dbms_lob.getlength(%1$s) = 0 or %1$s is null then '%2$s' else lower(dbms_crypto.hash(%1$s,2)) end", 
                                 columnName, EMPTY_STRING);
            case "mariadb" -> String.format("coalesce(md5(%1$s),'0'), '%2$s')", columnName, EMPTY_STRING);
            case "oracle" ->
                    String.format("case when dbms_lob.getlength(%1$s) = 0 or %1$s is null then '%2$s' else lower(dbms_crypto.hash(%1$s,2)) end", 
                                 columnName, EMPTY_STRING);
            default -> String.format("coalesce(md5(%1$s), '%2$s')", columnName, EMPTY_STRING);
        };
    }

    /**
     * Casts boolean data types to standardized format for comparison.
     *
     * @param dataType The database data type
     * @param columnName The column name to cast
     * @param platform The database platform
     * @return SQL expression for casting boolean data
     */
    public static String castBoolean(String dataType, String columnName, String platform) {
        String numberCast = Props.getProperty("number-cast");
        String numberFormat = Props.getProperty("standard-number-format");

        return switch (platform) {
            case "db2", "snowflake" -> String.format("coalesce(to_char(%1$s),'0')", columnName);
            case "oracle" -> String.format("nvl(to_char(%1$s),'0')", columnName);
            case "mariadb" ->
                    String.format("case when coalesce(cast(%1$s as char),'0') = 'true' then '1' else '0' end", columnName);
            case "mssql" ->
                    String.format("case when coalesce(cast(%1$s as varchar),'0') = 'true' then '1' else '0' end", columnName);
            case "mysql" ->
                    String.format("case when coalesce(convert(%1$s,char),'0') = 'true' then '1' else '0' end", columnName);
            default -> {
                String booleanConvert = String.format("case when coalesce(%s::text,'0') = 'true' then 1 else 0 end", columnName);
                yield NOTATION_CAST.equals(numberCast)
                        ? String.format("coalesce(trim(to_char(%s,'0.9999999999EEEE')),'%s')", booleanConvert, EMPTY_STRING)
                        : String.format("coalesce(trim(to_char(trim_scale(%s),'%s')),'%s')", booleanConvert, numberFormat, EMPTY_STRING);
            }
        };
    }

    /**
     * Casts numeric data types to standardized format for comparison.
     * Handles various numeric types including integers, floats, and decimals across different database platforms.
     *
     * @param dataType The database data type
     * @param columnName The column name to cast
     * @param platform The database platform
     * @return SQL expression for casting numeric data
     */
    public static String castNumber(String dataType, String columnName, String platform) {
        // DB2: smallint, integer/int, bigint, decimal(p,s)/numeric(p,s), real, double/double precision, decfloat
        // MariaDB/MySQL: tinyint, smallint, mediumint, int/integer, bigint, decimal(p,s)/numeric(p,s), float(p), double/double precision
        // MSSQL:  tinyint, smallint, in, bigint, decimal(p,s)/numeric(p,s), float(n), real, money/smallmoney
        // Oracle: number(p,s), float(p), binary_float, binary_double, integer/int, dec/decimal/numeric (aka number)
        // Postgres: smallint, integer/int, bigint, decimal(p,s)/numeric(p,s), real, double precision, serial/bigserial (aka auto incrementing int/bigint)
        // Snowflake: number, decimal, dec, numeric, int, integer, bigint, smallint, tinyint, byteint, float, float4, float8, double, double precision, real

        // Common Types:  These common types create buckets for categorizing numeric types into a common schema:
        //      Common Type     Description                     Use Case Example
        //      int16           2-byte integer                  Small counters, flags
        //      int32           4-byte integer                  IDs, general integers
        //      int64           8-byte integer                  Large IDs, big counters
        //      float32         4-byte floating point           Low-precision calculations
        //      float64         8-byte floating point           Scientific or precise floats
        //      decimal(p,s)    Arbitrary precision decimals    Money, accounting, exact math

        // Native type to common type mapping used for conversion and casting
        //      DBMS            Native Type(s)                  Common Type     Cast Method
        //      all             smallint                        int16           default/notation
        //      all             int, integer, mediumint         int32           default/notation
        //      all             bigint                          int64           default/notation
        //      postgres        real, float4                    float32         float32
        //      all             float, double, float8           float64         float64
        //      all             decimal(p,s), numeric(p,s)      decimal(p,s)    default/notation
        //      oracle          number(p,s)                     decimal(p,s)    default/notation
        //      mssql           tinyint                         <custom int8>   default/notation

        String numberCast = Props.getProperty("number-cast");

        return switch (dataType) {
            // float32
            // float64
            case "binary_float", "float", "float4", "real", "double", "binary_double", "float8" -> switch (platform) {
                case "db2" -> String.format("trim(to_char(%s,'999999999999999999999999999990.000'))", columnName);
                case "mariadb", "mysql", "mssql" ->
                        String.format("trim(cast(cast(%s as decimal(32,3)) as char))", columnName);
                case "oracle" ->
                        String.format("trim(to_char(cast(%s as NUMBER(32,3)),'99999999999999999999999999999990.000'))", columnName);
                default ->
                        String.format("trim(cast(cast(cast(%s as double precision) as numeric(32,3)) as text))", columnName);
            };
            default -> switch (platform) {
                case "db2" -> NOTATION_CAST.equals(numberCast)
                        ? String.format("CASE WHEN %1$s = 0 THEN '0.000000e+00' ELSE (CASE WHEN %1$s < 0 THEN '-' ELSE '' END) || substr(trim(char(CAST(round(abs(%1$s)/pow(10,floor(log10(abs(%1$s)))),6) AS float))),1,instr(trim(char(CAST(round(abs(%1$s)/pow(10,floor(log10(abs(%1$s)))),6) AS float))),'E')-1) || 'e' || (CASE WHEN floor(log10(abs(%1$s))) >= 0 THEN '+' ELSE '-' END) || lpad(trim(char(CAST(floor(log10(abs(%1$s))) AS integer))),2,'0') END", columnName)
                        : String.format("coalesce(trim(to_char(%s, '%s')),'%s')", columnName, Props.getProperty("standard-number-format"), EMPTY_STRING);
                case "mariadb", "mysql" -> NOTATION_CAST.equals(numberCast)
                        ? String.format("case when %1$s is null then '%2$s' else coalesce(if(%1$s=0,'0.0000000000e+00',concat(if(%1$s<0, '-', ''),format(abs(%1$s)/pow(10, floor(log10(abs(%1$s)))), 10),'e',if(floor(log10(abs(%1$s)))>=0,'+','-'),lpad(replace(replace(cast(FORMAT(floor(log10(abs(%1$s))), 2)/100 as char),'0.',''),'-',''),2,'0'))),'%2$s') end", columnName, EMPTY_STRING)
                        : String.format("case when %1$s is null then '%2$s' else coalesce(if(instr(cast(%1$s as char),'.')>0,concat(if(%1$s<0,'-',''),lpad(substring_index(cast(abs(%1$s) as char),'.',1),22,'0'),'.',rpad(substring_index(cast(%1$s as char),'.',-1),22,'0')),concat(if(%1$s<0,'-',''),lpad(cast(%1$s as char),22,'0'),'.',rpad('',22,'0'))),'%2$s') end", columnName, EMPTY_STRING);
                case "mssql" -> NOTATION_CAST.equals(numberCast)
                        ? String.format("lower(replace(coalesce(trim(format(%1$s,'E10')),'%2$s'),'E+0','e+'))", columnName, EMPTY_STRING)
                        : String.format("coalesce(cast(format(%s, '%s') as text),'%s')", columnName, Props.getProperty("standard-number-format"), EMPTY_STRING);
                case "oracle" -> NOTATION_CAST.equals(numberCast)
                        ? String.format("lower(nvl(trim(to_char(%1$s,'0.9999999999EEEE')),'%2$s'))", columnName, EMPTY_STRING)
                        : String.format("nvl(trim(to_char(%s,'%s')),'%s')", columnName, Props.getProperty("standard-number-format"), EMPTY_STRING);
                case "snowflake" -> NOTATION_CAST.equals(numberCast)
                        ? String.format("coalesce(trim(to_char(%1$s,'FM9.9999999999EEEE')),'%2$s')", columnName, EMPTY_STRING)
                        : String.format("trim(trim(coalesce(trim(to_char(%s, '%s')),'%s'),'0'))", columnName, Props.getProperty("standard-number-format"), EMPTY_STRING);
                default -> NOTATION_CAST.equals(numberCast)
                        ? String.format("coalesce(trim(to_char(%1$s,'0.9999999999EEEE')),'%2$s')", columnName, EMPTY_STRING)
                        : String.format("coalesce(trim(to_char(trim_scale(%1$s),'%s')),'%s')", columnName, Props.getProperty("standard-number-format"), EMPTY_STRING);
            };
        };

    }

    /**
     * Casts raw data types to standardized format for comparison.
     * When casting to raw, trims fixed length strings for all database platforms.
     *
     * @param dataType The database data type
     * @param columnName The column name to cast
     * @param platform The database platform
     * @return SQL expression for casting raw data
     */
    public static String castRaw(String dataType, String columnName, String platform) {
        // When casting to raw, need to trim fixed length strings for all dbms platforms
        String columnHashMethod = Props.getProperty("column-hash-method");

        return switch (platform) {
            case "db2", "mariadb", "mysql" -> RAW_HASH_METHOD.equals(columnHashMethod)
                    ? STRING_TYPES.contains(dataType)
                    ? String.format("trim(%s)", columnName)
                    : columnName
                    : String.format("case when length(%1$s)=0 then '%2$s' else %1$s end", columnName, EMPTY_STRING);
            case "mssql" -> RAW_HASH_METHOD.equals(columnHashMethod)
                    ? (STRING_TYPES.contains(dataType) && !TEXT_DATA_TYPE.equals(dataType))
                    ? String.format("trim(%s)", columnName)
                    : columnName
                    : String.format("coalesce(%1$s,'%2$s')", columnName, EMPTY_STRING);
            case "oracle" -> RAW_HASH_METHOD.equals(columnHashMethod)
                    ? STRING_TYPES.contains(dataType)
                    ? String.format("trim(%s)", columnName)
                    : columnName
                    : String.format("nvl(%1$s,'%2$s')", columnName, EMPTY_STRING);
            default -> RAW_HASH_METHOD.equals(columnHashMethod)
                    ? STRING_TYPES.contains(dataType)
                    ? String.format("trim(%s)", columnName)
                    : columnName
                    : String.format("coalesce(case when length(coalesce(%s::text,''))=0 then '%s' else %s::text end,'%s')", columnName, EMPTY_STRING, columnName, EMPTY_STRING);
        };
    }

    /**
     * Casts string data types to standardized format for comparison.
     * Handles various string types including text, varchar, and LOB types across different database platforms.
     *
     * @param dataType The database data type
     * @param columnName The column name to cast
     * @param platform The database platform
     * @param column JSON object containing column metadata
     * @return SQL expression for casting string data
     */
    public static String castString(String dataType, String columnName, String platform, JSONObject column) {
        switch (platform) {
            case "db2":
            case "mariadb":
            case "mysql":
                return column.getInt("dataLength") > 1
                        ? String.format("case when length(%1$s)=0 then '%2$s' else coalesce(trim(%1$s),'%2$s') end", columnName, EMPTY_STRING)
                        : String.format("case when length(%1$s)=0 then '%2$s' else trim(%1$s) end", columnName, EMPTY_STRING);
            case "mssql":
                // Cannot use trim on text data type.
                return TEXT_DATA_TYPE.equals(dataType)
                        ? String.format("coalesce(%1$s,'%2$s')", columnName, EMPTY_STRING)
                        : column.getInt("dataLength") > 1
                        ? String.format("case when len(%1$s)=0 then '%2$s' else coalesce(rtrim(ltrim(%1$s)),'%2$s') end", columnName, EMPTY_STRING)
                        : String.format("case when len(%1$s)=0 then '%2$s' else rtrim(ltrim(%1$s)) end", columnName, EMPTY_STRING);
            case "oracle":
                if (dataType.contains(LOB_DATA_TYPE)) {
                    return String.format("nvl(trim(to_char(%1$s)),'%2$s')", columnName, EMPTY_STRING);
                } else {
                    return column.getInt("dataLength") > 1
                            ? String.format("nvl(trim(%1$s),'%2$s')", columnName, EMPTY_STRING)
                            : String.format("nvl(%1$s,'%2$s')", columnName, EMPTY_STRING);
                }
            default:
                return String.format("coalesce(case when length(coalesce(trim(%1$s::text),''))=0 then '%2$s' else trim(%1$s::text) end,'%2$s')", columnName, EMPTY_STRING);
        }
    }

    /**
     * Casts timestamp data types to standardized format for comparison.
     * Handles various timestamp types including timezone-aware timestamps across different database platforms.
     *
     * @param dataType The database data type
     * @param columnName The column name to cast
     * @param platform The database platform
     * @return SQL expression for casting timestamp data
     */
    public static String castTimestamp(String dataType, String columnName, String platform) {
        return switch (platform) {
            case "db2" -> (dataType.contains(TIMEZONE_INDICATOR) || dataType.contains(TZ_INDICATOR))
                    ? String.format("coalesce(to_char(%1$s at time zone '%3$s','MMDDYYYYHH24MISS'),'%2$s')", columnName, EMPTY_STRING, UTC_TIMEZONE)
                    : String.format("coalesce(to_char(%1$s,'MMDDYYYYHH24MISS'),'%2$s')", columnName, EMPTY_STRING);
            case "mariadb" ->
                // Casting with factoring in session.time_zone seems to cause issues
                    String.format("coalesce(date_format(%1$s,'%%m%%d%%Y%%H%%i%%S'),'%2$s')", columnName, EMPTY_STRING);
            case "mssql" -> "date".equals(dataType)
                    ? String.format("coalesce(format(%1$s,'MMddyyyyHHmmss'),'%2$s')", columnName, EMPTY_STRING)
                    : String.format("coalesce(format(%1$s at time zone '%3$s','MMddyyyyHHmmss'),'%2$s')", columnName, EMPTY_STRING, UTC_TIMEZONE);
            case "mysql" -> (dataType.contains("timestamp") || dataType.contains("datetime"))
                    ? String.format("coalesce(date_format(convert_tz(%1$s,@@session.time_zone,'%3$s'),'%%m%%d%%Y%%H%%i%%S'),'%2$s')", columnName, EMPTY_STRING, UTC_TIMEZONE)
                    : String.format("coalesce(date_format(%1$s,'%%m%%d%%Y%%H%%i%%S'),'%2$s')", columnName, EMPTY_STRING);
            case "oracle" -> (dataType.contains(TIMEZONE_INDICATOR) || dataType.contains(TZ_INDICATOR))
                    ? String.format("nvl(to_char(%1$s at time zone '%3$s','MMDDYYYYHH24MISS'),'%2$s')", columnName, EMPTY_STRING, UTC_TIMEZONE)
                    : String.format("nvl(to_char(%1$s,'MMDDYYYYHH24MISS'),'%2$s')", columnName, EMPTY_STRING);
            default -> {
                boolean hasTZ = dataType.contains(TIMEZONE_INDICATOR) || dataType.contains(TZ_INDICATOR);
                yield hasTZ
                        ? String.format("coalesce(to_char(%1$s at time zone '%3$s','MMDDYYYYHH24MISS'),'%2$s')", columnName, EMPTY_STRING, UTC_TIMEZONE)
                        : String.format("coalesce(to_char(%1$s,'MMDDYYYYHH24MISS'),'%2$s')", columnName, EMPTY_STRING);
            }
        };
    }

}
