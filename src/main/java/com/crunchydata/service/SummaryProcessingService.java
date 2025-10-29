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

}
