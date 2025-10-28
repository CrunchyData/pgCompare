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

import com.crunchydata.config.ApplicationContext;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

/**
 * Service class for generating HTML reports and managing report content.
 * This class encapsulates the logic for creating HTML reports, managing
 * report layouts, and formatting report data.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class ReportGenerationService {
    
    private static final String THREAD_NAME = "report-generation";
    private static final DecimalFormat NUMBER_FORMATTER = new DecimalFormat("###,###,###,###,###");
    
    /**
     * Generate HTML report from report data.
     *
     * @param report Report content as JSON array
     * @param filePath HTML file name and location
     * @param title Report title
     */
    public static void generateHtmlReport(JSONArray report, String filePath, String title) {
        validateGenerateHtmlReportInputs(report, filePath, title);
        
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
     * Create a report section with title, data, and layout.
     *
     * @param title Section title
     * @param data Report data
     * @param layout Layout settings
     * @return JSON Object containing the section
     */
    public static JSONObject createSection(String title, Object data, JSONArray layout) {
        validateCreateSectionInputs(title, data, layout);
        
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
        validateGenerateCompleteReportInputs(context, tablesProcessed, runResult, stats);
        
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
     * Write HTML header with title and styles.
     * 
     * @param writer File writer
     * @param title Report title
     * @throws IOException if writing fails
     */
    private static void writeHtmlHeader(FileWriter writer, String title) throws IOException {
        writer.write(String.format("<html><head><title>%s</title>", title));
        writer.write("""
            <style>
                table {width: 100%; border-collapse: collapse;} th,
                td {border: 1px solid black; padding: 8px; text-align: left;}
                th {background-color: #f2f2f2;}
                .right-align { text-align: right; }
                .left-align  { text-align: left;}
                .center-align { text-align: center;}
            </style>
            """);
        writer.write("</head><body>");
    }
    
    /**
     * Write HTML content with report sections.
     * 
     * @param writer File writer
     * @param report Report data
     * @throws IOException if writing fails
     */
    private static void writeHtmlContent(FileWriter writer, JSONArray report) throws IOException {
        for (int i = 0; i < report.length(); i++) {
            JSONObject section = report.getJSONObject(i);
            JSONArray sectionData = section.getJSONArray("data");
            
            writer.write("<div/>");
            writer.write(String.format("<h2>%s</h2>", section.getString("title")));
            writer.write("<table>");
            
            // Write table header
            writeTableHeader(writer, section.getJSONArray("layout"));
            
            // Write table rows
            writeTableRows(writer, sectionData, section.getJSONArray("layout"));
            
            writer.write("</table>");
        }
    }
    
    /**
     * Write table header row.
     * 
     * @param writer File writer
     * @param layout Column layout
     * @throws IOException if writing fails
     */
    private static void writeTableHeader(FileWriter writer, JSONArray layout) throws IOException {
        writer.write("<tr>");
        for (int j = 0; j < layout.length(); j++) {
            JSONObject rowLayout = layout.getJSONObject(j);
            writer.write(String.format("<th>%s</th>", rowLayout.getString("columnHeader")));
        }
        writer.write("</tr>");
    }
    
    /**
     * Write table data rows.
     * 
     * @param writer File writer
     * @param sectionData Section data
     * @param layout Column layout
     * @throws IOException if writing fails
     */
    private static void writeTableRows(FileWriter writer, JSONArray sectionData, JSONArray layout) throws IOException {
        for (int j = 0; j < sectionData.length(); j++) {
            JSONObject row = sectionData.getJSONObject(j);
            writer.write("<tr>");
            for (int jr = 0; jr < layout.length(); jr++) {
                JSONObject rowLayout = layout.getJSONObject(jr);
                String cellValue = formatCellValue(row, rowLayout);
                writer.write(String.format("<td class=\"%s\">%s</td>", 
                    rowLayout.getString("columnClass"), cellValue));
            }
            writer.write("</tr>");
        }
    }
    
    /**
     * Format cell value based on layout settings.
     * 
     * @param row Data row
     * @param rowLayout Column layout
     * @return Formatted cell value
     */
    private static String formatCellValue(JSONObject row, JSONObject rowLayout) {
        String columnKey = rowLayout.getString("columnKey");
        Object value = row.get(columnKey);
        
        if (rowLayout.getBoolean("commaFormat") && value instanceof Number) {
            return NUMBER_FORMATTER.format(((Number) value).longValue());
        }
        
        return value.toString();
    }
    
    /**
     * Write HTML footer.
     * 
     * @param writer File writer
     * @throws IOException if writing fails
     */
    private static void writeHtmlFooter(FileWriter writer) throws IOException {
        writer.write("</body></html>");
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
                .put("elapsedTime", NUMBER_FORMATTER.format(stats.getElapsedTime()))
                .put("totalRows", NUMBER_FORMATTER.format(stats.getTotalRows()))
                .put("outOfSyncRows", NUMBER_FORMATTER.format(stats.getOutOfSyncRows()))
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
     * Validate inputs for generateHtmlReport method.
     * 
     * @param report Report data
     * @param filePath File path
     * @param title Report title
     */
    private static void validateGenerateHtmlReportInputs(JSONArray report, String filePath, String title) {
        if (report == null) {
            throw new IllegalArgumentException("Report data cannot be null");
        }
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }
        if (title == null || title.trim().isEmpty()) {
            throw new IllegalArgumentException("Report title cannot be null or empty");
        }
    }
    
    /**
     * Validate inputs for createSection method.
     * 
     * @param title Section title
     * @param data Section data
     * @param layout Section layout
     */
    private static void validateCreateSectionInputs(String title, Object data, JSONArray layout) {
        if (title == null || title.trim().isEmpty()) {
            throw new IllegalArgumentException("Section title cannot be null or empty");
        }
        if (data == null) {
            throw new IllegalArgumentException("Section data cannot be null");
        }
        if (layout == null) {
            throw new IllegalArgumentException("Section layout cannot be null");
        }
    }
    
    /**
     * Validate inputs for generateCompleteReport method.
     * 
     * @param context Application context
     * @param tablesProcessed Number of tables processed
     * @param runResult Run results
     * @param stats Summary statistics
     */
    private static void validateGenerateCompleteReportInputs(ApplicationContext context, int tablesProcessed, 
                                                           JSONArray runResult, SummaryStatistics stats) {
        if (context == null) {
            throw new IllegalArgumentException("Application context cannot be null");
        }
        if (tablesProcessed < 0) {
            throw new IllegalArgumentException("Tables processed cannot be negative");
        }
        if (runResult == null) {
            throw new IllegalArgumentException("Run result cannot be null");
        }
        if (stats == null) {
            throw new IllegalArgumentException("Summary statistics cannot be null");
        }
    }
    
    /**
     * Inner class to hold summary statistics.
     */
    public static class SummaryStatistics {
        private final long totalRows;
        private final long outOfSyncRows;
        private final long elapsedTime;
        
        public SummaryStatistics(long totalRows, long outOfSyncRows, long elapsedTime) {
            this.totalRows = totalRows;
            this.outOfSyncRows = outOfSyncRows;
            this.elapsedTime = elapsedTime;
        }
        
        public long getTotalRows() { return totalRows; }
        public long getOutOfSyncRows() { return outOfSyncRows; }
        public long getElapsedTime() { return elapsedTime; }
        public long getThroughput() { return elapsedTime > 0 ? totalRows / elapsedTime : totalRows; }
    }
}
