package com.crunchydata.services;

import org.json.JSONObject;

import java.util.Arrays;

import com.crunchydata.util.Logging;
import static com.crunchydata.util.Settings.Props;

public class ColumnValidation {

    public static String[] supportedDataTypes = new String[]{"bool","boolean","binary_double","binary_float","bpchar","char","clob","date","float","int", "int2", "int4", "int8", "numeric","number","text", "timestamp","timestamp(0)","timestamp(1) with time zone", "timestamp(3)", "timestamp(3) with time zone", "timestamp(6)", "timestamp(6) with time zone","timestamp(9)","timestamp(9) with time zone","varchar","varchar2"};
    public static String[] booleanTypes = new String[]{"bool","boolean"};
    public static String[] charTypes = new String[]{"bpchar","char","clob","text","varchar","varchar2"};
    public static String[] numericTypes = new String[]{"bigint", "bigserial", "binary_double","binary_float", "decimal", "double precision", "float","int", "integer", "int2", "int4", "int8", "number", "numeric", "real", "serial", "smallint", "smallserial"};
    public static String[] timestampTypes = new String[]{"date","timestamp","timestamp(0)","timestamp(1) with time zone", "timestamp(3)", "timestamp(3) with time zone", "timestamp(6)", "timestamp(6) with time zone","timestamp(9)","timestamp(9) with time zone"};
    public static String[] restrictedDataTypes = new String[]{"clob"};

    public static String columnValueMap(String platform, JSONObject column) {
        String oraTemp;
        String pgTemp;
        String dt;

        if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) && column.getInt("dataScale") == 0 ) {
            dt="numeric";
            // TODO: Need to compare the source and target data type to reduce the column/hash size.  Problem is some platforms return null for scale so there is no way to know if integer or not.
            //oraTemp = "nvl(to_char("+column.getString("columnName")+"),' ')";
            //pgTemp = "coalesce(" +column.getString("columnName") + "::text,' ')";
            oraTemp = "nvl(to_char(" + column.getString("columnName") + ",'0000000000000000000000.0000000000000000000000'),' ')";
            pgTemp = "coalesce(to_char(trim_scale(" + column.getString("columnName") + "),'0000000000000000000000.0000000000000000000000'),' ')";
        } else if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) ) {
            dt = "numeric";
            oraTemp = "nvl(to_char(" + column.getString("columnName") + ",'0000000000000000000000.0000000000000000000000'),' ')";
            pgTemp = "coalesce(to_char(trim_scale(" + column.getString("columnName") + "),'0000000000000000000000.0000000000000000000000'),' ')";
        } else if ( Arrays.asList(booleanTypes).contains(column.getString("dataType").toLowerCase()) ) {
            dt = "boolean";
            // TODO: Problem converting when source is not boolean type but target is.
            oraTemp = "nvl(to_char(" + column.getString("columnName") + "),'0')";
            pgTemp = "case when coalesce(" + column.getString("columnName") + "::text,'0') = 'true' then '1' else '0' end";
        } else if ( Arrays.asList(timestampTypes).contains(column.getString("dataType").toLowerCase()) ) {
            oraTemp = "nvl(to_char(" + column.getString("columnName") + ",'MMDDYYYYHH24MISS'),' ')";
            pgTemp = "coalesce(to_char(" + column.getString("columnName") + ",'MMDDYYYYHH24MISS'),' ')";
        } else if ( Arrays.asList(charTypes).contains(column.getString("dataType").toLowerCase()) ) {
            if (column.getInt("dataLength") > 1) {
                oraTemp = "nvl(trim(" + column.getString("columnName") + "),' ')";
            } else {
                oraTemp = "nvl(" + column.getString("columnName") + ",' ')";
            }
            pgTemp = "coalesce(" + column.getString("columnName") + "::text,' ')";
        } else {
            oraTemp = column.getString("columnName");
            pgTemp = column.getString("columnName");
        }

        return ( platform.equals("postgres") ? pgTemp : oraTemp );

    }

}

