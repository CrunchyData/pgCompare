package com.crunchydata.controller;

import com.crunchydata.util.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileWriter;
import java.text.DecimalFormat;

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
    
}
