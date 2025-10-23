package com.crunchydata.controller;

import com.crunchydata.ApplicationContext;
import com.crunchydata.util.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileWriter;
import java.text.DecimalFormat;
import java.util.List;

public class ReportController {

    private static final String THREAD_NAME = "report-ctrl";

    /**
     *
     * @param title     Section title
     * @param data      Report data
     * @param layout    Layout settings
     * @return          JSON Object
     */
    public static JSONObject createSection(String title, Object data, JSONArray layout) {
        return new JSONObject()
                .put("title", title)
                .put("data", data)
                .put("layout", layout);
    }

    /**
     * Generate HTML report.
     *
     * @param report        Report content.
     * @param filePath      HTML file name and location.
     * @param title         Report title.
     */
    public static void generateHtmlReport(JSONArray report, String filePath, String title) {
        Logging.write("info","main", String.format("Generating HTML report:  %s...", filePath));

        DecimalFormat df = new DecimalFormat("###,###,###,###,###");

        try {
            FileWriter writer = new FileWriter(filePath);

            writer.write(String.format("<html><head><title>%s</title>",title));
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

            for (int i = 0; i < report.length(); i++) {
                JSONObject section = report.getJSONObject(i);
                JSONArray sectionData = section.getJSONArray("data");

                writer.write("<div/>");
                writer.write(String.format("<h2>%s</h2>", section.getString("title")));
                writer.write("<table>");

                // Header
                writer.write("<tr>");

                for (int j =0; j < section.getJSONArray("layout").length(); j++) {
                    JSONObject rowLayout = section.getJSONArray("layout").getJSONObject(j);
                    writer.write(String.format("<th>%s</th>", rowLayout.getString("columnHeader")));
                }
                writer.write("</tr>");

                for (int j = 0; j < sectionData.length(); j++) {
                    JSONObject row = sectionData.getJSONObject(j);
                    writer.write("<tr>");
                    for (int jr =0; jr < section.getJSONArray("layout").length(); jr++) {
                        JSONObject rowLayout = section.getJSONArray("layout").getJSONObject(jr);
                        writer.write(String.format("<td class=\"%s\">%s</td>", rowLayout.getString("columnClass"), (rowLayout.getBoolean("commaFormat")) ? df.format(row.getInt(rowLayout.getString("columnKey"))) : row.get(rowLayout.getString("columnKey")).toString()));
                    }
                    writer.write("</tr>");
                }
                writer.write("</table>");
            }

            writer.write("</body></html>");
            writer.close();
        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", THREAD_NAME, String.format("Error generating report at line %s:  %s", stackTrace[0].getLineNumber(), e.getMessage()));
        }
    }
    
    // Constants for summary generation
    private static final String THREAD_NAME_SUMMARY = "summary";
    private static final int MIN_ELAPSED_TIME_SECONDS = 1;
    private static final int DEFAULT_INDENT = 0;
    private static final int TABLE_INDENT = 4;
    private static final int SUMMARY_INDENT = 2;
    private static final int DETAIL_INDENT = 8;
    
    /**
     * Create and display summary of the comparison operation.
     * 
     * @param context Application context
     * @param tablesProcessed Number of tables processed
     * @param runResult JSON array containing results for each table
     * @param isCheck Whether this was a recheck operation
     */
    public static void createSummary(ApplicationContext context, int tablesProcessed, JSONArray runResult, boolean isCheck) {
        printSummary("Summary: ", DEFAULT_INDENT);

        if (tablesProcessed > 0) {
            SummaryStatistics stats = calculateSummaryStatistics(runResult, context.getStartStopWatch());
            displayTableSummaries(runResult);
            displayJobSummary(tablesProcessed, stats);
            
            if (context.isGenReport()) {
                generateReport(context, tablesProcessed, runResult, stats, isCheck);
            }
        } else {
            displayNoTablesMessage(isCheck);
        }
    }
    
    /**
     * Calculate summary statistics from the run results.
     * 
     * @param runResult JSON array containing results for each table
     * @param startStopWatch Start time of the operation
     * @return SummaryStatistics object containing calculated statistics
     */
    private static SummaryStatistics calculateSummaryStatistics(JSONArray runResult, long startStopWatch) {
        long endStopWatch = System.currentTimeMillis();
        long totalRows = 0;
        long outOfSyncRows = 0;
        
        // Calculate totals from all table results
        for (int i = 0; i < runResult.length(); i++) {
            JSONObject result = runResult.getJSONObject(i);
            int nbrEqual = result.getInt("equal");
            int notEqual = result.getInt("notEqual");
            int missingSource = result.getInt("missingSource");
            int missingTarget = result.getInt("missingTarget");

            totalRows += nbrEqual + notEqual + missingSource + missingTarget;
            outOfSyncRows += notEqual + missingSource + missingTarget;
        }
        
        long elapsedTime = Math.max((endStopWatch - startStopWatch) / 1000, MIN_ELAPSED_TIME_SECONDS);
        
        return new SummaryStatistics(totalRows, outOfSyncRows, elapsedTime);
    }
    
    /**
     * Display individual table summaries.
     * 
     * @param runResult JSON array containing results for each table
     */
    private static void displayTableSummaries(JSONArray runResult) {
        for (int i = 0; i < runResult.length(); i++) {
            JSONObject result = runResult.getJSONObject(i);
            
            printSummary(String.format("TABLE: %s", result.getString("tableName")), TABLE_INDENT);
            printSummary(String.format("Table Summary: Status         = %s", result.getString("compareStatus")), DETAIL_INDENT);
            printSummary(String.format("Table Summary: Equal          = %19d", result.getInt("equal")), DETAIL_INDENT);
            printSummary(String.format("Table Summary: Not Equal      = %19d", result.getInt("notEqual")), DETAIL_INDENT);
            printSummary(String.format("Table Summary: Missing Source = %19d", result.getInt("missingSource")), DETAIL_INDENT);
            printSummary(String.format("Table Summary: Missing Target = %19d", result.getInt("missingTarget")), DETAIL_INDENT);
        }
    }
    
    /**
     * Display job summary statistics.
     * 
     * @param tablesProcessed Number of tables processed
     * @param stats Summary statistics
     */
    private static void displayJobSummary(int tablesProcessed, SummaryStatistics stats) {
        DecimalFormat df = new DecimalFormat("###,###,###,###,###");
        
        printSummary("Job Summary: ", DEFAULT_INDENT);
        printSummary(String.format("Tables Processed               = %s", tablesProcessed), SUMMARY_INDENT);
        printSummary(String.format("Elapsed Time (seconds)         = %s", df.format(stats.getElapsedTime())), SUMMARY_INDENT);
        printSummary(String.format("Total Rows Processed           = %s", df.format(stats.getTotalRows())), SUMMARY_INDENT);
        printSummary(String.format("Total Out-of-Sync              = %s", df.format(stats.getOutOfSyncRows())), SUMMARY_INDENT);
        printSummary(String.format("Through-put (rows/per second)  = %s", df.format(stats.getThroughput())), SUMMARY_INDENT);
    }
    
    /**
     * Generate HTML report if requested.
     * 
     * @param context Application context
     * @param tablesProcessed Number of tables processed
     * @param runResult JSON array containing results for each table
     * @param stats Summary statistics
     * @param isCheck Whether this was a recheck operation
     */
    private static void generateReport(ApplicationContext context, int tablesProcessed, JSONArray runResult, SummaryStatistics stats, boolean isCheck) {
        DecimalFormat df = new DecimalFormat("###,###,###,###,###");
        
        // Create job summary JSON
        JSONObject jobSummary = new JSONObject()
                .put("tablesProcessed", df.format(tablesProcessed))
                .put("elapsedTime", df.format(stats.getElapsedTime()))
                .put("totalRows", df.format(stats.getTotalRows()))
                .put("outOfSyncRows", df.format(stats.getOutOfSyncRows()))
                .put("rowsPerSecond", df.format(stats.getThroughput()));

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
     * Create job summary layout for the report.
     * 
     * @return JSONArray containing column definitions
     */
    private static JSONArray createJobSummaryLayout() {
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
    private static JSONArray createRunResultLayout() {
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
     * Add check results to the report if this was a recheck operation.
     * 
     * @param reportArray The report array to add results to
     * @param runResult JSON array containing results for each table
     */
    private static void addCheckResultsToReport(JSONArray reportArray, JSONArray runResult) {
        JSONArray runCheckResultLayout = new JSONArray(List.of(
                createReportColumn("Primary Key", "pk", "left-align", false),
                createReportColumn("Status", "compareStatus", "left-align", false),
                createReportColumn("Result", "compareResult", "left-align", false)
        ));

        for (int i = 0; i < runResult.length(); i++) {
            JSONObject tableResult = runResult.getJSONObject(i);
            if (tableResult.has("checkResult")) {
                String tableName = tableResult.getString("tableName");
                JSONArray checkData = tableResult.getJSONObject("checkResult").getJSONArray("data");
                reportArray.put(createSection(String.format("Table: %s", tableName), checkData, runCheckResultLayout));
            }
        }
    }
    
    /**
     * Display message when no tables were processed.
     * 
     * @param isCheck Whether this was a recheck operation
     */
    private static void displayNoTablesMessage(boolean isCheck) {
        String message = isCheck ? 
            "No out of sync records found" : 
            "No tables were processed. Need to do discovery? Used correct batch nbr?";
        Logging.write("warning", THREAD_NAME_SUMMARY, message);
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
    private static JSONObject createReportColumn(String header, String key, String align, boolean commaFormat) {
        return new JSONObject()
                .put("columnHeader", header)
                .put("columnClass", align)
                .put("columnKey", key)
                .put("commaFormat", commaFormat);
    }
    
    /**
     * Print summary message with indentation.
     * 
     * @param message Message to print
     * @param indent Number of spaces to indent
     */
    private static void printSummary(String message, int indent) {
        Logging.write("info", THREAD_NAME_SUMMARY, " ".repeat(indent) + message);
    }
    
    /**
     * Inner class to hold summary statistics.
     */
    private static class SummaryStatistics {
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
        public long getThroughput() { return totalRows / elapsedTime; }
    }
    
}
