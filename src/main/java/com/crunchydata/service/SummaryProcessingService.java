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

import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.DecimalFormat;

/**
 * Service class for processing summary statistics and calculations.
 * This class encapsulates the logic for calculating summary statistics,
 * processing run results, and managing summary data.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class SummaryProcessingService {
    
    private static final String THREAD_NAME = "summary-processing";
    private static final DecimalFormat NUMBER_FORMATTER = new DecimalFormat("###,###,###,###,###");
    private static final int MIN_ELAPSED_TIME_SECONDS = 1;
    
    /**
     * Calculate summary statistics from the run results.
     * 
     * @param runResult JSON array containing results for each table
     * @param startStopWatch Start time of the operation
     * @return SummaryStatistics object containing calculated statistics
     */
    public static ReportGenerationService.SummaryStatistics calculateSummaryStatistics(JSONArray runResult, long startStopWatch) {
        validateCalculateSummaryStatisticsInputs(runResult, startStopWatch);
        
        long totalRows = 0;
        long outOfSyncRows = 0;
        long elapsedTime = Math.max((System.currentTimeMillis() - startStopWatch) / 1000, MIN_ELAPSED_TIME_SECONDS);
        
        for (int i = 0; i < runResult.length(); i++) {
            JSONObject tableResult = runResult.getJSONObject(i);
            
            if (tableResult.has("totalRows")) {
                totalRows += tableResult.getLong("totalRows");
            }
            
            if (tableResult.has("notEqual") || tableResult.has("missingSource") || tableResult.has("missingTarget")) {
                long notEqual = tableResult.optLong("notEqual", 0);
                long missingSource = tableResult.optLong("missingSource", 0);
                long missingTarget = tableResult.optLong("missingTarget", 0);
                outOfSyncRows += notEqual + missingSource + missingTarget;
            }
        }
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Summary calculated: %d total rows, %d out of sync rows, %d seconds elapsed", 
                totalRows, outOfSyncRows, elapsedTime));
        
        return new ReportGenerationService.SummaryStatistics(totalRows, outOfSyncRows, elapsedTime);
    }
    
    /**
     * Display table summaries in the console.
     * 
     * @param runResult JSON array containing results for each table
     */
    public static void displayTableSummaries(JSONArray runResult) {
        validateDisplayTableSummariesInputs(runResult);
        
        LoggingUtils.write("info", THREAD_NAME, "Table Summaries:");
        
        for (int i = 0; i < runResult.length(); i++) {
            JSONObject tableResult = runResult.getJSONObject(i);
            displayTableSummary(tableResult);
        }
    }
    
    /**
     * Display job summary with overall statistics.
     * 
     * @param tablesProcessed Number of tables processed
     * @param stats Summary statistics
     */
    public static void displayJobSummary(int tablesProcessed, ReportGenerationService.SummaryStatistics stats) {
        validateDisplayJobSummaryInputs(tablesProcessed, stats);
        
        LoggingUtils.write("info", THREAD_NAME, "Job Summary:");
        LoggingUtils.write("info", THREAD_NAME,
            String.format("  Tables Processed: %s", NUMBER_FORMATTER.format(tablesProcessed)));
        LoggingUtils.write("info", THREAD_NAME,
            String.format("  Total Rows: %s", NUMBER_FORMATTER.format(stats.getTotalRows())));
        LoggingUtils.write("info", THREAD_NAME,
            String.format("  Out of Sync Rows: %s", NUMBER_FORMATTER.format(stats.getOutOfSyncRows())));
        LoggingUtils.write("info", THREAD_NAME,
            String.format("  Elapsed Time: %s seconds", NUMBER_FORMATTER.format(stats.getElapsedTime())));
        LoggingUtils.write("info", THREAD_NAME,
            String.format("  Throughput: %s rows/second", NUMBER_FORMATTER.format(stats.getThroughput())));
    }
    
    /**
     * Display message when no tables were processed.
     * 
     * @param isCheck Whether this was a recheck operation
     */
    public static void displayNoTablesMessage(boolean isCheck) {
        String message = isCheck ? 
            "No out of sync records found" : 
            "No tables were processed. Need to do discovery? Used correct batch nbr?";
        LoggingUtils.write("warning", THREAD_NAME, message);
    }
    
    /**
     * Display summary message with indentation.
     * 
     * @param message Message to print
     * @param indent Number of spaces to indent
     */
    public static void printSummary(String message, int indent) {
        validatePrintSummaryInputs(message, indent);
        LoggingUtils.write("info", THREAD_NAME, " ".repeat(indent) + message);
    }
    
    /**
     * Display individual table summary.
     * 
     * @param tableResult Table result JSON object
     */
    private static void displayTableSummary(JSONObject tableResult) {
        String tableName = tableResult.optString("tableName", "Unknown");
        String status = tableResult.optString("compareStatus", "Unknown");
        long elapsedTime = tableResult.optLong("elapsedTime", 0);
        long totalRows = tableResult.optLong("totalRows", 0);
        long equal = tableResult.optLong("equal", 0);
        long notEqual = tableResult.optLong("notEqual", 0);
        long missingSource = tableResult.optLong("missingSource", 0);
        long missingTarget = tableResult.optLong("missingTarget", 0);
        
        long throughput = elapsedTime > 0 ? totalRows / elapsedTime : totalRows;
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("  Table: %s, Status: %s, Time: %ds, Rows: %s, Equal: %s, Not Equal: %s, Missing Source: %s, Missing Target: %s, Throughput: %s rows/sec",
                tableName, status, elapsedTime, 
                NUMBER_FORMATTER.format(totalRows),
                NUMBER_FORMATTER.format(equal),
                NUMBER_FORMATTER.format(notEqual),
                NUMBER_FORMATTER.format(missingSource),
                NUMBER_FORMATTER.format(missingTarget),
                NUMBER_FORMATTER.format(throughput)));
    }
    
    /**
     * Validate inputs for calculateSummaryStatistics method.
     * 
     * @param runResult Run results
     * @param startStopWatch Start time
     */
    private static void validateCalculateSummaryStatisticsInputs(JSONArray runResult, long startStopWatch) {
        if (runResult == null) {
            throw new IllegalArgumentException("Run result cannot be null");
        }
        if (startStopWatch <= 0) {
            throw new IllegalArgumentException("Start stop watch must be positive");
        }
    }
    
    /**
     * Validate inputs for displayTableSummaries method.
     * 
     * @param runResult Run results
     */
    private static void validateDisplayTableSummariesInputs(JSONArray runResult) {
        if (runResult == null) {
            throw new IllegalArgumentException("Run result cannot be null");
        }
    }
    
    /**
     * Validate inputs for displayJobSummary method.
     * 
     * @param tablesProcessed Number of tables processed
     * @param stats Summary statistics
     */
    private static void validateDisplayJobSummaryInputs(int tablesProcessed, ReportGenerationService.SummaryStatistics stats) {
        if (tablesProcessed < 0) {
            throw new IllegalArgumentException("Tables processed cannot be negative");
        }
        if (stats == null) {
            throw new IllegalArgumentException("Summary statistics cannot be null");
        }
    }
    
    /**
     * Validate inputs for printSummary method.
     * 
     * @param message Message to print
     * @param indent Indentation level
     */
    private static void validatePrintSummaryInputs(String message, int indent) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        if (indent < 0) {
            throw new IllegalArgumentException("Indent cannot be negative");
        }
    }
}
