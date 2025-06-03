package com.crunchydata.util;

import org.json.JSONObject;

import static com.crunchydata.util.ColumnUtility.*;
import static com.crunchydata.util.Settings.Props;

public class CastUtility {

    public static String cast(String dataType, String columnName, String platform, JSONObject column) {
        if (BOOLEAN_TYPES.contains(dataType)) return castBoolean(dataType, columnName, platform);
        if (BINARY_TYPES.contains(dataType)) return castBinary(dataType, columnName, platform);
        if (NUMERIC_TYPES.contains(dataType)) return castNumber(dataType, columnName, platform);
        if (STRING_TYPES.contains(dataType)) return castString(dataType, columnName, platform, column);
        if (TIMESTAMP_TYPES.contains(dataType)) return castTimestamp(dataType, columnName, platform);
        return castRaw(dataType, columnName, platform);
    }


    public static String castBinary(String dataType, String columnName, String platform) {
        switch (platform) {
            // MSSQL does not have a binary type.
            // Postgres and MySQL use the same function for binary types.
            case "db2":
                return String.format("case when dbms_lob.getlength(%1$s) = 0 or %1$s is null then ' ' else lower(dbms_crypto.hash(%1$s,2)) end", columnName);
            case "mariadb":
                return String.format("coalesce(md5(%1$s),'0'), ' ')", columnName);
            case "oracle":
                return String.format("case when dbms_lob.getlength(%1$s) = 0 or %1$s is null then ' ' else lower(dbms_crypto.hash(%1$s,2)) end", columnName);
            default:
                return String.format("coalesce(md5(%1$s), ' ')", columnName);
        }
    }

    public static String castBoolean(String dataType, String columnName, String platform) {
        String numberCast = Props.getProperty("number-cast");
        String numberFormat = Props.getProperty("standard-number-format");

        switch (platform) {
            case "db2":
            case "oracle":
                return String.format("nvl(to_char(%1$s),'0')", columnName);
            case "mariadb":
                return String.format("case when coalesce(cast(%1$s as char),'0') = 'true' then '1' else '0' end", columnName);
            case "mssql":
                return String.format("case when coalesce(cast(%1$s as varchar),'0') = 'true' then '1' else '0' end", columnName);
            case "mysql":
                return String.format("case when coalesce(convert(%1$s,char),'0') = 'true' then '1' else '0' end", columnName);
            default:
                String booleanConvert = String.format("case when coalesce(%s::text,'0') = 'true' then 1 else 0 end", columnName);
                return "notation".equals(numberCast)
                        ? String.format("coalesce(trim(to_char(%s,'0.9999999999EEEE')),' ')", booleanConvert)
                        : String.format("coalesce(trim(to_char(trim_scale(%s),'%s')),' ')", booleanConvert, numberFormat);
        }

    }

    public static String castNumber(String dataType, String columnName, String platform) {
        // DB2: smallint, integer/int, bigint, decimal(p,s)/numeric(p,s), real, double/double precision, decfloat
        // MariaDB/MySQL: tinyint, smallint, mediumint, int/integer, bigint, decimal(p,s)/numeric(p,s), float(p), double/double precision
        // MSSQL:  tinyint, smallint, in, bigint, decimal(p,s)/numeric(p,s), float(n), real, money/smallmoney
        // Oracle: number(p,s), float(p), binary_float, binary_double, integer/int, dec/decimal/numeric (aka number)
        // Postgres: smallint, integer/int, bigint, decimal(p,s)/numeric(p,s), real, double precision, serial/bigserial (aka auto incrementing int/bigint)

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

        String floatCast = Props.getProperty("float-cast");
        String numberCast = Props.getProperty("number-cast");
        String numberFormat = Props.getProperty("standard-number-format");
        String columnHashMethod = Props.getProperty("column-hash-method");

        switch (dataType) {
            // float32 casts
            case "binary_float":
            case "float":
            case "float4":
            case "real":
                switch (platform) {
                    case "db2":
                        return "char".equals(floatCast)
                                ? String.format("case when abs(%1$s) < 1 then '0' else '' end|| trim(trailing '.' from trim(trailing '0' from cast(cast(cast(%1$s as double) as decimal(30,18)) as varchar(1000))))", columnName)
                                : String.format("CASE WHEN %1$s = 0 THEN '0.000000e+00' ELSE (CASE WHEN %1$s < 0 THEN '-' ELSE '' END) || substr(trim(char(CAST(round(abs(%1$s)/pow(10,floor(log10(abs(%1$s)))),6) AS float))),1,instr(trim(char(CAST(round(abs(%1$s)/pow(10,floor(log10(abs(%1$s)))),6) AS float))),'E')-1) || 'e' || (CASE WHEN floor(log10(abs(%1$s))) >= 0 THEN '+' ELSE '-' END) || lpad(trim(char(CAST(floor(log10(abs(%1$s))) AS integer))),2,'0') END",columnName);
                    case "mariadb":
                    case "mysql":
                        return "char".equals(floatCast)
                                ? String.format("cast(cast(%1$s as double) as char)", columnName)
                                : String.format("coalesce(if(%1$s=0,'0.000000e+00',concat(if(%1$s<0, '-', ''),format(abs(%1$s)/pow(10, floor(log10(abs(%1$s)))), 6),'e',if(floor(log10(abs(%1$s)))>=0,'+','-'),lpad(replace(replace(cast(FORMAT(floor(log10(abs(%1$s))), 2)/100 as char),'0.',''),'-',''),2,'0'))),' ')", columnName);
                    case "mssql":
                        return "char".equals(floatCast)
                                ? String.format("rtrim(rtrim(cast(cast(%1$s as DECIMAL(30,7)) as varchar(1000)),'0'),'.')", columnName)
                                : String.format("replace(replace(lower(coalesce(trim(format(%1$s,'E6')),' ')),'e-00','e-0'),'e+00','e+0')", columnName);
                    case "oracle":
                        return "char".equals(floatCast)
                                ? String.format("to_char(cast(%1$s as double precision))", columnName)
                                : String.format("lower(nvl(trim(to_char(%1$s,'0.999999EEEE')),' '))", columnName);
                    default:
                        // If not comparing to MSSQL, then fall through to float64 formating for postgres (default)
                        if ("mssql".equals(Props.getProperty("source-target-type")) || "mssql".equals(Props.getProperty("target-target-type"))) {
                            return "char".equals(floatCast)
                                    ? String.format("trim(trailing '.' from trim(trailing '0' from cast(cast(cast(%1$s as float8) as decimal(30,7)) as text)))", columnName)
                                    : String.format("coalesce(trim(to_char(%1$s,'0.999999EEEE')),' ')", columnName);
                        }
                }

                // float64 casts
            case "double":
            case "binary_double":
            case "float8":
                switch (platform) {
                    case "db2":
                        return "char".equals(floatCast)
                                ? String.format("case when abs(%1$s) < 1 then '0' else '' end|| trim(trailing '.' from trim(trailing '0' from cast(cast(cast(%1$s as double) as decimal(30,18)) as varchar(1000))))", columnName)
                                : String.format("CASE WHEN %1$s = 0 THEN '0.000000e+00' ELSE (CASE WHEN %1$s < 0 THEN '-' ELSE '' END) || substr(trim(char(CAST(round(abs(%1$s)/pow(10,floor(log10(abs(%1$s)))),6) AS float))),1,instr(trim(char(CAST(round(abs(%1$s)/pow(10,floor(log10(abs(%1$s)))),6) AS float))),'E')-1) || 'e' || (CASE WHEN floor(log10(abs(%1$s))) >= 0 THEN '+' ELSE '-' END) || lpad(trim(char(CAST(floor(log10(abs(%1$s))) AS integer))),2,'0') END",columnName);
                    case "mariadb":
                    case "mysql":
                        return "char".equals(floatCast)
                                ? String.format("cast(cast(%1$s as double) as char)", columnName)
                                : String.format("coalesce(if(%1$s=0,'0.000000e+00',concat(if(%1$s<0, '-', ''),format(abs(%1$s)/pow(10, floor(log10(abs(%1$s)))), 6),'e',if(floor(log10(abs(%1$s)))>=0,'+','-'),lpad(replace(replace(cast(FORMAT(floor(log10(abs(%1$s))), 2)/100 as char),'0.',''),'-',''),2,'0'))),' ')", columnName);
                    case "mssql":
                        return "char".equals(floatCast)
                                ? String.format("rtrim(rtrim(cast(cast(%1$s as DECIMAL(30,7)) as varchar(1000)),'0'),'.')", columnName)
                                : String.format("replace(replace(lower(coalesce(trim(format(%1$s,'E6')),' ')),'e-00','e-0'),'e+00','e+0')", columnName);
                    case "oracle":
                        return String.format("lower(nvl(trim(to_char(%1$s,'0.999999EEEE')),' '))", columnName);
                    default:
                        return "char".equals(floatCast)
                                ? String.format("cast(cast(%1$s as double precision) as text)", columnName)
                                : String.format("coalesce(trim(to_char(%1$s,'0.999999EEEE')),' ')", columnName);
                }

            default:

                switch (platform) {
                    case "db2":
                        return "notation".equals(numberCast)
                                ? String.format("CASE WHEN %1$s = 0 THEN '0.000000e+00' ELSE (CASE WHEN %1$s < 0 THEN '-' ELSE '' END) || substr(trim(char(CAST(round(abs(%1$s)/pow(10,floor(log10(abs(%1$s)))),6) AS float))),1,instr(trim(char(CAST(round(abs(%1$s)/pow(10,floor(log10(abs(%1$s)))),6) AS float))),'E')-1) || 'e' || (CASE WHEN floor(log10(abs(%1$s))) >= 0 THEN '+' ELSE '-' END) || lpad(trim(char(CAST(floor(log10(abs(%1$s))) AS integer))),2,'0') END",columnName)
                                : String.format("nvl(trim(to_char(%s, '%s')),' ')", columnName, Props.getProperty("standard-number-format"));
                    case "mariadb":
                    case "mysql":
                        return "notation".equals(numberCast)
                                ? String.format("case when %1$s is null then ' ' else coalesce(if(%1$s=0,'0.0000000000e+00',concat(if(%1$s<0, '-', ''),format(abs(%1$s)/pow(10, floor(log10(abs(%1$s)))), 10),'e',if(floor(log10(abs(%1$s)))>=0,'+','-'),lpad(replace(replace(cast(FORMAT(floor(log10(abs(%1$s))), 2)/100 as char),'0.',''),'-',''),2,'0'))),' ') end", columnName)
                                : String.format("case when %1$s is null then ' ' else coalesce(if(instr(cast(%1$s as char),'.')>0,concat(if(%1$s<0,'-',''),lpad(substring_index(cast(abs(%1$s) as char),'.',1),22,'0'),'.',rpad(substring_index(cast(%1$s as char),'.',-1),22,'0')),concat(if(%1$s<0,'-',''),lpad(cast(%1$s as char),22,'0'),'.',rpad('',22,'0'))),' ') end", columnName);
                    case "mssql":
                        return "notation".equals(numberCast)
                                ? String.format("lower(replace(coalesce(trim(format(%1$s,'E10')),' '),'E+0','e+'))", columnName)
                                : String.format("coalesce(cast(format(%s, '%s') as text),' ')", columnName, Props.getProperty("standard-number-format"));
                    case "oracle":
                        return "notation".equals(numberCast)
                                ? String.format("lower(nvl(trim(to_char(%1$s,'0.9999999999EEEE')),' '))", columnName)
                                : String.format("nvl(trim(to_char(%s,'%s')),' ')", columnName, Props.getProperty("standard-number-format"));
                    default:
                        return "notation".equals(numberCast)
                                ? String.format("coalesce(trim(to_char(%1$s,'0.9999999999EEEE')),' ')", columnName)
                                : String.format("coalesce(trim(to_char(trim_scale(%1$s),'" + Props.getProperty("standard-number-format") + "')),' ')", columnName);

                }

        }

    }

    public static String castRaw(String dataType, String columnName, String platform) {
        // When casting to raw, need to trim fixed length strings for all dbms platforms
        String floatCast = Props.getProperty("float-cast");
        String numberCast = Props.getProperty("number-cast");
        String numberFormat = Props.getProperty("standard-number-format");
        String columnHashMethod = Props.getProperty("column-hash-method");

        switch (platform) {
            case "db2":
            case "mariadb":
            case "mysql":
                return "raw".equals(columnHashMethod)
                        ? STRING_TYPES.contains(dataType)
                            ? String.format("trim(%s)",columnName)
                            : columnName
                        : String.format("case when length(%1$s)=0 then ' ' else %1$s end", columnName);

            case "mssql":
                return "raw".equals(columnHashMethod)
                        ? (STRING_TYPES.contains(dataType) && !dataType.equals("text"))
                            ? String.format("trim(%s)",columnName)
                            : columnName
                        : String.format("coalesce(%1$s,' ')", columnName);
            case "oracle":
                return "raw".equals(columnHashMethod)
                        ? STRING_TYPES.contains(dataType)
                            ? String.format("trim(%s)",columnName)
                            : columnName
                        : String.format("nvl(%1$s,' ')", columnName);
            default:
                return "raw".equals(columnHashMethod)
                        ? STRING_TYPES.contains(dataType)
                            ? String.format("trim(%s)",columnName)
                            : columnName
                        : String.format("coalesce(case when length(coalesce(%s::text,''))=0 then ' ' else %s::text end,' ')",columnName, columnName);

        }

    }

    public static String castString(String dataType, String columnName, String platform, JSONObject column) {
        switch (platform) {
            case "db2":
            case "maridb":
            case "mysql":
                return column.getInt("dataLength") > 1
                        ? String.format("case when length(%1$s)=0 then ' ' else coalesce(trim(%1$s),' ') end", columnName)
                        : String.format("case when length(%1$s)=0 then ' ' else trim(%1$s) end", columnName);
            case "mssql":
                // Cannot use trim on text data type.
                return "text".equals(dataType)
                        ? String.format("coalesce(%1$s,' ')", columnName)
                        : column.getInt("dataLength") > 1
                        ? String.format("case when len(%1$s)=0 then ' ' else coalesce(rtrim(ltrim(%1$s)),' ') end", columnName)
                        : String.format("case when len(%1$s)=0 then ' ' else rtrim(ltrim(%1$s)) end", columnName);
            case "oracle":
                if (dataType.contains("lob")) {
                    return String.format("nvl(trim(to_char(%1$s)),' ')", columnName);
                } else {
                    return column.getInt("dataLength") > 1
                            ? String.format("nvl(trim(%1$s),' ')", columnName)
                            : String.format("nvl(%1$s,' ')", columnName);
                }
            default:
                return String.format("coalesce(case when length(coalesce(trim(%1$s::text),''))=0 then ' ' else trim(%1$s::text) end,' ')", columnName);
        }

    }

    public static String castTimestamp(String dataType, String columnName, String platform) {
        switch (platform) {
            case "db2":
                return (dataType.contains("time zone") || dataType.contains("tz"))
                        ? String.format("nvl(to_char(%1$s at time zone 'UTC','MMDDYYYYHH24MISS'),' ')", columnName)
                        : String.format("nvl(to_char(%1$s,'MMDDYYYYHH24MISS'),' ')", columnName);
            case "mariadb":
                // Casting with factoring in session.time_zone seems to cause issues
                return String.format("coalesce(date_format(%1$s,'%m%d%Y%H%i%S'),' ')", columnName);

            case "mssql":
                return "date".equals(dataType)
                        ? String.format("coalesce(format(%1$s,'MMddyyyyHHmmss'),' ')", columnName)
                        : String.format("coalesce(format(%1$s at time zone 'UTC','MMddyyyyHHmmss'),' ')", columnName);
            case "mysql":
                return (dataType.contains("timestamp") || dataType.contains("datetime"))
                        ? "coalesce(date_format(convert_tz(" + columnName + ",@@session.time_zone,'UTC'),'%m%d%Y%H%i%S'),' ')"
                        : "coalesce(date_format(" + columnName + ",'%m%d%Y%H%i%S'),' ')";
            case "oracle":
                return (dataType.contains("time zone") || dataType.contains("tz"))
                        ? String.format("nvl(to_char(%1$s at time zone 'UTC','MMDDYYYYHH24MISS'),' ')", columnName)
                        : String.format("nvl(to_char(%1$s,'MMDDYYYYHH24MISS'),' ')", columnName);
            default:
                boolean hasTZ = dataType.contains("time zone") || dataType.contains("tz");
                return hasTZ
                        ? String.format("coalesce(to_char(%1$s at time zone 'UTC','MMDDYYYYHH24MISS'),' ')", columnName)
                        : String.format("coalesce(to_char(%1$s,'MMDDYYYYHH24MISS'),' ')", columnName);
        }

    }

}
