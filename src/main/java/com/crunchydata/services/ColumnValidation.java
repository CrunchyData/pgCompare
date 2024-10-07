/*
 * Copyright 2012-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crunchydata.services;

import java.util.Arrays;

/**
 * Utility class for column data type validation and classification.
 *
 * <p>Provides methods to determine the classification of a given database column data type.</p>
 *
 * @author Brian Pace
 */
public class ColumnValidation {

    /**
     * Array of data types classified as boolean.
     */
    public static String[] booleanTypes = new String[]{"bool", "boolean"};

    /**
     * Array of data types classified as character.
     */
    public static String[] charTypes = new String[]{"bpchar", "char", "character", "clob", "json", "jsonb", "nchar", "nclob",
            "ntext", "nvarchar", "nvarchar2", "text", "varchar", "varchar2", "xml"};

    /**
     * Array of data types classified as numeric.
     */
    public static String[] numericTypes = new String[]{"bigint", "bigserial", "binary_double", "binary_float", "dec",
            "decimal", "double", "double precision", "fixed", "float", "float4", "float8", "int", "integer", "int2",
            "int4", "int8", "money", "number", "numeric", "real", "serial", "smallint", "smallmoney", "smallserial",
            "tinyint"};

    /**
     * Array of data types classified as timestamp.
     */
    public static String[] timestampTypes = new String[]{"date", "datetime", "datetimeoffset", "datetime2",
            "smalldatetime", "time", "timestamp", "timestamptz", "timestamp(0)", "timestamp(1) with time zone",
            "timestamp(3)", "timestamp(3) with time zone", "timestamp(6)", "timestamp(6) with time zone",
            "timestamp(9)", "timestamp(9) with time zone", "year"};

    /**
     * Array of data types classified as binary.
     */
    public static String[] binaryTypes = new String[]{"bytea", "binary", "blob", "raw", "varbinary"};

    /**
     * Array of unsupported data types.
     */
    public static String[] unsupportedDataTypes = new String[]{"bfile", "bit", "cursor", "enum", "hierarchyid",
            "image", "rowid", "rowversion", "set", "sql_variant", "uniqueidentifier", "long", "long raw"};

    /**
     * Returns the classification of a given database column data type.
     *
     * <p>The method checks the provided dataType against predefined arrays of data types and returns
     * a classification string.</p>
     *
     * <p>If the dataType does not match any predefined type, it defaults to "char".</p>
     *
     * @param dataType The database column data type to classify.
     * @return A string representing the classification of the data type ("boolean", "numeric", "char").
     */
    public static String getDataClass(String dataType) {
        // Default classification is "char"
        String dataClass = "char";

        if (Arrays.asList(booleanTypes).contains(dataType.toLowerCase())) {
            dataClass = "boolean";
        }

        if (Arrays.asList(numericTypes).contains(dataType.toLowerCase())) {
            dataClass = "numeric";
        }

        if (Arrays.asList(timestampTypes).contains(dataType.toLowerCase())) {
            dataClass = "char";
        }

        return dataClass;
    }

}

