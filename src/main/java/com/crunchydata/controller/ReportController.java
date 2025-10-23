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

import com.crunchydata.ApplicationContext;
import com.crunchydata.util.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Controller class that provides a simplified interface for report operations.
 * This class has been refactored to use specialized services for better separation
 * of concerns and improved maintainability.
 *
 * @author Brian Pace
 * @version 1.0
 */
public class ReportController {

    private static final String THREAD_NAME = "report-ctrl";
    
    // Constants for summary generation
    private static final int DEFAULT_INDENT = 0;

    /**
     * Create a report section with title, data, and layout using the optimized ReportGenerationService.
     *
     * @param title     Section title
     * @param data      Report data
     * @param layout    Layout settings
     * @return          JSON Object
     */
    public static JSONObject createSection(String title, Object data, JSONArray layout) {
        return ReportGenerationService.createSection(title, data, layout);
    }

    /**
     * Generate HTML report using the optimized ReportGenerationService.
     *
     * @param report        Report content
     * @param filePath      HTML file name and location
     * @param title         Report title
     */
    public static void generateHtmlReport(JSONArray report, String filePath, String title) {
        try {
            ReportGenerationService.generateHtmlReport(report, filePath, title);
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error generating HTML report: %s", e.getMessage()));
            throw new RuntimeException("Failed to generate HTML report", e);
        }
    }
    
    /**
     * Create and display summary of the comparison operation using optimized services.
     * 
     * @param context Application context
     * @param tablesProcessed Number of tables processed
     * @param runResult JSON array containing results for each table
     * @param isCheck Whether this was a recheck operation
     */
    public static void createSummary(ApplicationContext context, int tablesProcessed, JSONArray runResult, boolean isCheck) {
        try {
            validateCreateSummaryInputs(context, tablesProcessed, runResult);
            
            DisplayOperationsService.printSummary("Summary: ", DEFAULT_INDENT);

            if (tablesProcessed > 0) {
                ReportGenerationService.SummaryStatistics stats = SummaryProcessingService.calculateSummaryStatistics(runResult, context.getStartStopWatch());
                DisplayOperationsService.displayTableSummaries(runResult);
                DisplayOperationsService.displayJobSummary(tablesProcessed, stats);
                
                if (context.isGenReport()) {
                    ReportGenerationService.generateCompleteReport(context, tablesProcessed, runResult, stats, isCheck);
                }
            } else {
                DisplayOperationsService.displayNoTablesMessage(isCheck);
            }
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error creating summary: %s", e.getMessage()));
            throw new RuntimeException("Failed to create summary", e);
        }
    }
    
    /**
     * Validate inputs for createSummary method.
     * 
     * @param context Application context
     * @param tablesProcessed Number of tables processed
     * @param runResult Run results
     */
    private static void validateCreateSummaryInputs(ApplicationContext context, int tablesProcessed, JSONArray runResult) {
        if (context == null) {
            throw new IllegalArgumentException("Application context cannot be null");
        }
        if (tablesProcessed < 0) {
            throw new IllegalArgumentException("Tables processed cannot be negative");
        }
        if (runResult == null) {
            throw new IllegalArgumentException("Run result cannot be null");
        }
    }
}