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
 * Service class for display operations and formatting.
 * This class encapsulates the logic for displaying information,
 * formatting output, and managing console display operations.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class DisplayOperationsService {
    
    private static final String THREAD_NAME = "display-operations";
    private static final DecimalFormat NUMBER_FORMATTER = new DecimalFormat("###,###,###,###,###");
    
    // Indentation constants
    private static final int DEFAULT_INDENT = 0;
    private static final int TABLE_INDENT = 4;
    private static final int SUMMARY_INDENT = 2;
    private static final int DETAIL_INDENT = 8;
    
    /**
     * Display table summaries with proper formatting.
     * 
     * @param runResult JSON array containing results for each table
     */
    public static void displayTableSummaries(JSONArray runResult) {
        validateDisplayTableSummariesInputs(runResult);
        
        printSummary("Table Summaries:", TABLE_INDENT);
        
        for (int i = 0; i < runResult.length(); i++) {
            JSONObject tableResult = runResult.getJSONObject(i);
            displayIndividualTableSummary(tableResult, TABLE_INDENT);
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
        
        printSummary("Job Summary:", SUMMARY_INDENT);
        printSummary(String.format("Tables Processed: %s", NUMBER_FORMATTER.format(tablesProcessed)), DETAIL_INDENT);
        printSummary(String.format("Total Rows: %s", NUMBER_FORMATTER.format(stats.getTotalRows())), DETAIL_INDENT);
        printSummary(String.format("Out of Sync Rows: %s", NUMBER_FORMATTER.format(stats.getOutOfSyncRows())), DETAIL_INDENT);
        printSummary(String.format("Elapsed Time: %s seconds", NUMBER_FORMATTER.format(stats.getElapsedTime())), DETAIL_INDENT);
        printSummary(String.format("Throughput: %s rows/second", NUMBER_FORMATTER.format(stats.getThroughput())), DETAIL_INDENT);
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
     * Display individual table summary with detailed information.
     * 
     * @param tableResult Table result JSON object
     * @param indent Indentation level
     */
    public static void displayIndividualTableSummary(JSONObject tableResult, int indent) {
        validateDisplayIndividualTableSummaryInputs(tableResult, indent);
        
        String tableName = tableResult.optString("tableName", "Unknown");
        String status = tableResult.optString("compareStatus", "Unknown");
        long elapsedTime = tableResult.optLong("elapsedTime", 0);
        long totalRows = tableResult.optLong("totalRows", 0);
        long equal = tableResult.optLong("equal", 0);
        long notEqual = tableResult.optLong("notEqual", 0);
        long missingSource = tableResult.optLong("missingSource", 0);
        long missingTarget = tableResult.optLong("missingTarget", 0);
        
        long throughput = elapsedTime > 0 ? totalRows / elapsedTime : totalRows;
        
        printSummary(String.format("Table: %s", tableName), indent);
        printSummary(String.format("  Status: %s", status), indent + 2);
        printSummary(String.format("  Elapsed Time: %d seconds", elapsedTime), indent + 2);
        printSummary(String.format("  Total Rows: %s", NUMBER_FORMATTER.format(totalRows)), indent + 2);
        printSummary(String.format("  Equal Rows: %s", NUMBER_FORMATTER.format(equal)), indent + 2);
        printSummary(String.format("  Not Equal Rows: %s", NUMBER_FORMATTER.format(notEqual)), indent + 2);
        printSummary(String.format("  Missing Source: %s", NUMBER_FORMATTER.format(missingSource)), indent + 2);
        printSummary(String.format("  Missing Target: %s", NUMBER_FORMATTER.format(missingTarget)), indent + 2);
        printSummary(String.format("  Throughput: %s rows/second", NUMBER_FORMATTER.format(throughput)), indent + 2);
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
     * Format number with comma separators.
     * 
     * @param number Number to format
     * @return Formatted number string
     */
    public static String formatNumber(long number) {
        return NUMBER_FORMATTER.format(number);
    }
    
    /**
     * Format number with comma separators.
     * 
     * @param number Number to format
     * @return Formatted number string
     */
    public static String formatNumber(int number) {
        return NUMBER_FORMATTER.format(number);
    }
    
    /**
     * Calculate throughput (rows per second).
     * 
     * @param totalRows Total number of rows processed
     * @param elapsedTime Elapsed time in seconds
     * @return Throughput in rows per second
     */
    public static long calculateThroughput(long totalRows, long elapsedTime) {
        return elapsedTime > 0 ? totalRows / elapsedTime : totalRows;
    }
    
    /**
     * Calculate throughput (rows per second).
     * 
     * @param totalRows Total number of rows processed
     * @param elapsedTime Elapsed time in seconds
     * @return Throughput in rows per second
     */
    public static long calculateThroughput(int totalRows, long elapsedTime) {
        return elapsedTime > 0 ? totalRows / elapsedTime : totalRows;
    }
    
    /**
     * Create a formatted summary line.
     * 
     * @param label Label for the value
     * @param value Value to display
     * @param indent Indentation level
     * @return Formatted summary line
     */
    public static String createSummaryLine(String label, Object value, int indent) {
        validateCreateSummaryLineInputs(label, value, indent);
        
        String formattedValue = (value instanceof Number) ? 
            NUMBER_FORMATTER.format(((Number) value).longValue()) : 
            value.toString();
        
        return " ".repeat(indent) + String.format("%s: %s", label, formattedValue);
    }
    
    /**
     * Display a formatted summary section.
     * 
     * @param title Section title
     * @param indent Indentation level
     */
    public static void displaySectionTitle(String title, int indent) {
        validateDisplaySectionTitleInputs(title, indent);
        printSummary(title, indent);
    }
    
    /**
     * Display a separator line.
     * 
     * @param length Length of the separator
     * @param character Character to use for separator
     */
    public static void displaySeparator(int length, char character) {
        validateDisplaySeparatorInputs(length, character);
        String separator = String.valueOf(character).repeat(length);
        LoggingUtils.write("info", THREAD_NAME, separator);
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
     * Validate inputs for displayIndividualTableSummary method.
     * 
     * @param tableResult Table result
     * @param indent Indentation level
     */
    private static void validateDisplayIndividualTableSummaryInputs(JSONObject tableResult, int indent) {
        if (tableResult == null) {
            throw new IllegalArgumentException("Table result cannot be null");
        }
        if (indent < 0) {
            throw new IllegalArgumentException("Indent cannot be negative");
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
    
    /**
     * Validate inputs for createSummaryLine method.
     * 
     * @param label Label
     * @param value Value
     * @param indent Indentation level
     */
    private static void validateCreateSummaryLineInputs(String label, Object value, int indent) {
        if (label == null || label.trim().isEmpty()) {
            throw new IllegalArgumentException("Label cannot be null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        if (indent < 0) {
            throw new IllegalArgumentException("Indent cannot be negative");
        }
    }
    
    /**
     * Validate inputs for displaySectionTitle method.
     * 
     * @param title Section title
     * @param indent Indentation level
     */
    private static void validateDisplaySectionTitleInputs(String title, int indent) {
        if (title == null || title.trim().isEmpty()) {
            throw new IllegalArgumentException("Section title cannot be null or empty");
        }
        if (indent < 0) {
            throw new IllegalArgumentException("Indent cannot be negative");
        }
    }
    
    /**
     * Validate inputs for displaySeparator method.
     * 
     * @param length Separator length
     * @param character Separator character
     */
    private static void validateDisplaySeparatorInputs(int length, char character) {
        if (length <= 0) {
            throw new IllegalArgumentException("Separator length must be positive");
        }
    }
}
