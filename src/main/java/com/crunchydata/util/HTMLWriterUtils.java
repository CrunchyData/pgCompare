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

package com.crunchydata.util;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Service class for generating HTML reports and managing report content.
 * This class encapsulates the logic for creating HTML reports, managing
 * report layouts, and formatting report data.
 * 
 * @author Brian Pace
 */
public class HTMLWriterUtils {
    
    private static final DecimalFormat NUMBER_FORMATTER = new DecimalFormat("###,###,###,###,###");
    
    /**
     * Write HTML content with report sections.
     * 
     * @param writer File writer
     * @param report Report data
     * @throws IOException if writing fails
     */
    public static void writeHtmlContent(FileWriter writer, JSONArray report) throws IOException {
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
     * Write HTML header with title and styles.
     *
     * @param writer File writer
     * @param title Report title
     * @throws IOException if writing fails
     */
    public static void writeHtmlHeader(FileWriter writer, String title) throws IOException {
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
     * Write table header row.
     * 
     * @param writer File writer
     * @param layout Column layout
     * @throws IOException if writing fails
     */
    public static void writeTableHeader(FileWriter writer, JSONArray layout) throws IOException {
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
    public static void writeTableRows(FileWriter writer, JSONArray sectionData, JSONArray layout) throws IOException {
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
    public static void writeHtmlFooter(FileWriter writer) throws IOException {
        writer.write("</body></html>");
    }
}
