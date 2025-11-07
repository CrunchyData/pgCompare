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

package com.crunchydata.controller;

import com.crunchydata.core.database.ColumnMetadataBuilder;
import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONObject;

import static com.crunchydata.service.DatabaseMetadataService.*;

/**
 * ColumnController class that provides a simplified interface for column operations.
 * This class has been refactored to use specialized services for better separation
 * of concerns and improved maintainability.
 *
 * @author Brian Pace
 */
public class ColumnController {
    private static final String THREAD_NAME = "column-ctrl";

    /**
     * Retrieves column metadata for a given table using the optimized ColumnMetadataBuilder.
     *
     * @param columnMap        JSON object containing column mapping information
     * @param targetType       The target type of columns
     * @param platform         The database platform (e.g., "mssql", "mysql")
     * @param schema           The schema of the table
     * @param table            The name of the table
     * @param useDatabaseHash  Flag to determine whether to use database hash
     * @return                 A ColumnMetadata object containing column information
     */
    public static ColumnMetadata getColumnInfo(JSONObject columnMap, String targetType, String platform, 
                                             String schema, String table, Boolean useDatabaseHash) {
        try {
            // Create builder with platform-specific settings
            String concatOperator = getConcatOperator(platform);
            String quoteChar = getQuoteChar(platform);
            String replaceSyntax = getReplacePKSyntax(platform);
            
            ColumnMetadataBuilder builder = new ColumnMetadataBuilder(
                targetType, platform, schema, table, useDatabaseHash, concatOperator, quoteChar, replaceSyntax);
            
            // Build and return metadata
            return builder.build(columnMap);
            
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Error building column metadata for %s.%s: %s", schema, table, e.getMessage()));
            throw new RuntimeException("Failed to build column metadata", e);
        }
    }

}
