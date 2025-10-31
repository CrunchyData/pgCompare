/*
 * Copyright 2012-2025 the original author or authors.
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

package com.crunchydata.service;

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.util.LoggingUtils;

import static com.crunchydata.config.Settings.Props;
import static com.crunchydata.service.DatabaseMetadataService.buildLoadSQL;

public class SQLSyntaxService {

    private static final String THREAD_NAME = "sql-syntax-srv";

    /**
     * Generate compare SQL for source and target.
     *
     * @param dctmSource Source table map
     * @param dctmTarget Target table map
     * @param ciSource Source column metadata
     * @param ciTarget Target column metadata
     */
    public static void generateCompareSQL(DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                                           ColumnMetadata ciSource, ColumnMetadata ciTarget) {
        String method = Props.getProperty("column-hash-method");
        dctmSource.setCompareSQL(buildLoadSQL(method, dctmSource, ciSource));
        dctmTarget.setCompareSQL(buildLoadSQL(method, dctmTarget, ciTarget));

        LoggingUtils.write("info", THREAD_NAME, "(source) Compare SQL: " + dctmSource.getCompareSQL());
        LoggingUtils.write("info", THREAD_NAME, "(target) Compare SQL: " + dctmTarget.getCompareSQL());
    }
}
