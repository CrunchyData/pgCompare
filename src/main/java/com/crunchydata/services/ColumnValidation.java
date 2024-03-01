package com.crunchydata.services;

import java.util.Arrays;

public class ColumnValidation {

    public static String[] booleanTypes = new String[]{"bool","boolean"};
    public static String[] charTypes = new String[]{"bpchar","char","clob","json","jsonb","nchar","nclob","ntext","nvarchar","nvarchar2","text","varchar","varchar2","xml"};
    public static String[] numericTypes = new String[]{"bigint", "bigserial", "binary_double","binary_float", "dec", "decimal", "double", "double precision", "fixed", "float", "float4", "float8", "int", "integer", "int2", "int4", "int8","money","number", "numeric", "real", "serial", "smallint","smallmoney","smallserial","tinyint"};
    public static String[] timestampTypes = new String[]{"date","datetime","datetimeoffset","datetime2","smalldatetime","time","timestamp","timestamptz","timestamp(0)","timestamp(1) with time zone", "timestamp(3)", "timestamp(3) with time zone", "timestamp(6)", "timestamp(6) with time zone","timestamp(9)","timestamp(9) with time zone","year"};
    public static String[] binaryTypes = new String[]{"bytea","binary","blob","raw","varbinary"};
    public static String[] unsupportedDataTypes = new String[]{"bfile","bit","cursor","enum","hierarchyid","image","rowid","rowversion","set","sql_variant","uniqueidentifier","long","long raw"};

    public static String getDataClass(String dataType) {
        String dataClass = "char";

        if ( Arrays.asList(booleanTypes).contains(dataType) ) {
            dataClass = "boolean";
        }

        if ( Arrays.asList(numericTypes).contains(dataType) ) {
            dataClass = "numeric";
        }

        if ( Arrays.asList(timestampTypes).contains(dataType) ) {
            dataClass = "char";
        }

        return dataClass;
    }

}

