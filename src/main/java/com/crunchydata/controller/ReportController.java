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

import com.crunchydata.config.ApplicationContext;
import com.crunchydata.util.DisplayOperations;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import static com.crunchydata.util.HTMLWriterUtils.*;

/**
 * Controller class that provides a simplified interface for report operations.
 * This class has been refactored to use specialized services for better separation
 * of concerns and improved maintainability.
 *
 * @author Brian Pace
 */
public class ReportController {

    private static final String THREAD_NAME = "report-ctrl";
    
    // Constants for summary generation
    private static final int DEFAULT_INDENT = 0;
    private static final DecimalFormat NUMBER_FORMATTER = new DecimalFormat("###,###,###,###,###");
    private static final int MIN_ELAPSED_TIME_SECONDS = 1;

    /**
     * Generate HTML report from report data.
     *
     * @param report Report content as JSON array
     * @param filePath HTML file name and location
     * @param title Report title
     */
    public static void generateHtmlReport(JSONArray report, String filePath, String title) {
        LoggingUtils.write("info", THREAD_NAME, String.format("Generating HTML report: %s", filePath));

        try (FileWriter writer = new FileWriter(filePath)) {
            writeHtmlHeader(writer, title);
            writeHtmlContent(writer, report);
            writeHtmlFooter(writer);

            LoggingUtils.write("info", THREAD_NAME, "HTML report generated successfully");

        } catch (IOException e) {
            LoggingUtils.write("severe", THREAD_NAME,
                    String.format("Error generating HTML report: %s", e.getMessage()));
            throw new RuntimeException("Failed to generate HTML report", e);
        }
    }

    /**
     * Generate complete report with job summary and table results.
     *
     * @param context Application context
     * @param tablesProcessed Number of tables processed
     * @param runResult JSON array containing results for each table
     * @param stats Summary statistics
     * @param isCheck Whether this was a recheck operation
     */
    public static void generateCompleteReport(ApplicationContext context, int tablesProcessed,
                                              JSONArray runResult, SummaryStatistics stats, boolean isCheck) {

        // Create job summary JSON
        JSONObject jobSummary = createJobSummaryData(tablesProcessed, stats);

        // Create report layouts
        JSONArray jobSummaryLayout = createJobSummaryLayout();
        JSONArray runResultLayout = createRunResultLayout();

        // Build report array
        JSONArray reportArray = new JSONArray()
                .put(createSection("Job Summary", new JSONArray().put(jobSummary), jobSummaryLayout))
                .put(createSection("Table Summary", runResult, runResultLayout));

        // Add check results if this was a recheck operation
        if (isCheck) {
            addCheckResultsToReport(reportArray, runResult);
        }

        generateHtmlReport(reportArray, context.getReportFileName(), "pgCompare Summary");
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
            DisplayOperations.printSummary("Summary: ", DEFAULT_INDENT);

            if (tablesProcessed > 0) {
                SummaryStatistics stats = calculateSummaryStatistics(runResult, context.getStartStopWatch());
                DisplayOperations.displayTableSummaries(runResult);
                DisplayOperations.displayJobSummary(tablesProcessed, stats);
                
                if (context.isGenReport()) {
                    generateCompleteReport(context, tablesProcessed, runResult, stats, isCheck);
                }
            } else {
                DisplayOperations.displayNoTablesMessage(isCheck);
            }
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Error creating summary: %s", e.getMessage()));
            throw new RuntimeException("Failed to create summary", e);
        }
    }

    /**
     * Calculate summary statistics from the run results.
     *
     * @param runResult JSON array containing results for each table
     * @param startStopWatch Start time of the operation
     * @return SummaryStatistics object containing calculated statistics
     */
    public static SummaryStatistics calculateSummaryStatistics(JSONArray runResult, long startStopWatch) {
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

        return new SummaryStatistics(totalRows, outOfSyncRows, elapsedTime);
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
     * Create a report section with title, data, and layout.
     *
     * @param title Section title
     * @param data Report data
     * @param layout Layout settings
     * @return JSON Object containing the section
     */
    public static JSONObject createSection(String title, Object data, JSONArray layout) {
        return new JSONObject()
                .put("title", title)
                .put("data", data)
                .put("layout", layout);
    }

    /**
     * Create job summary layout for the report.
     *
     * @return JSONArray containing column definitions
     */
    public static JSONArray createJobSummaryLayout() {
        return new JSONArray(List.of(
                createReportColumn("Tables Processed", "tablesProcessed", "right-align", false),
                createReportColumn("Elapsed Time", "elapsedTime", "right-align", false),
                createReportColumn("Rows per Second", "rowsPerSecond", "right-align", false),
                createReportColumn("Total Rows", "totalRows", "right-align", false),
                createReportColumn("Out of Sync Rows", "outOfSyncRows", "right-align", false)
        ));
    }

    /**
     * Create run result layout for the report.
     *
     * @return JSONArray containing column definitions
     */
    public static JSONArray createRunResultLayout() {
        return new JSONArray(List.of(
                createReportColumn("Table", "tableName", "left-align", false),
                createReportColumn("Compare Status", "compareStatus", "left-align", false),
                createReportColumn("Elapsed Time", "elapsedTime", "right-align", true),
                createReportColumn("Rows per Second", "rowsPerSecond", "right-align", true),
                createReportColumn("Rows Total", "totalRows", "right-align", true),
                createReportColumn("Rows Equal", "equal", "right-align", true),
                createReportColumn("Rows Not Equal", "notEqual", "right-align", true),
                createReportColumn("Rows Missing on Source", "missingSource", "right-align", true),
                createReportColumn("Rows Missing on Target", "missingTarget", "right-align", true)
        ));
    }

    /**
     * Create check result layout for recheck operations.
     *
     * @return JSONArray containing column definitions
     */
    public static JSONArray createCheckResultLayout() {
        return new JSONArray(List.of(
                createReportColumn("Primary Key", "pk", "left-align", false),
                createReportColumn("Status", "compareStatus", "left-align", false),
                createReportColumn("Result", "compareResult", "left-align", false)
        ));
    }

    /**
     * Create job summary data object.
     *
     * @param tablesProcessed Number of tables processed
     * @param stats Summary statistics
     * @return Job summary JSON object
     */
    private static JSONObject createJobSummaryData(int tablesProcessed, SummaryStatistics stats) {
        return new JSONObject()
                .put("tablesProcessed", NUMBER_FORMATTER.format(tablesProcessed))
                .put("elapsedTime", NUMBER_FORMATTER.format(stats.elapsedTime()))
                .put("totalRows", NUMBER_FORMATTER.format(stats.totalRows()))
                .put("outOfSyncRows", NUMBER_FORMATTER.format(stats.outOfSyncRows()))
                .put("rowsPerSecond", NUMBER_FORMATTER.format(stats.getThroughput()));
    }

    /**
     * Add check results to the report if this was a recheck operation.
     *
     * @param reportArray The report array to add results to
     * @param runResult JSON array containing results for each table
     */
    private static void addCheckResultsToReport(JSONArray reportArray, JSONArray runResult) {
        JSONArray checkResultLayout = createCheckResultLayout();

        for (int i = 0; i < runResult.length(); i++) {
            JSONObject tableResult = runResult.getJSONObject(i);
            if (tableResult.has("checkResult")) {
                String tableName = tableResult.getString("tableName");
                JSONArray checkData = tableResult.getJSONObject("checkResult").getJSONArray("data");
                reportArray.put(createSection(String.format("Table: %s", tableName), checkData, checkResultLayout));
            }
        }
    }

    /**
     * Create a report column definition.
     *
     * @param header Column header text
     * @param key JSON key for the column data
     * @param align CSS alignment class
     * @param commaFormat Whether to apply comma formatting
     * @return JSONObject containing column definition
     */
    public static JSONObject createReportColumn(String header, String key, String align, boolean commaFormat) {
        return new JSONObject()
                .put("columnHeader", header)
                .put("columnClass", align)
                .put("columnKey", key)
                .put("commaFormat", commaFormat);
    }

    /**
     * Inner class to hold summary statistics.
     */
    public record SummaryStatistics(long totalRows, long outOfSyncRows, long elapsedTime) {
        public long getThroughput() {
            return elapsedTime > 0 ? totalRows / elapsedTime : totalRows;
        }
    }

}