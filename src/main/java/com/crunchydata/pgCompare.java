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

package com.crunchydata;

import com.crunchydata.config.ApplicationContext;
import com.crunchydata.util.CommandLineParser;
import com.crunchydata.util.LoggingUtils;
import org.apache.commons.cli.CommandLine;

/**
 * Main class for pgCompare, a tool for comparing and reconciling database tables.
 * This class provides the entry point for database comparison operations including
 * discovery, comparison, and reconciliation of database tables.
 * 
 *
 * @author Brian Pace
 */
public class pgCompare {
    
    private static final String THREAD_NAME = "main";

    /**
     * Main entry point for the pgCompare application.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        try {
            // Parse command line arguments
            CommandLine cmd = CommandLineParser.parse(args);
            if (cmd == null) {
                return; // Help or version was displayed
            }
            
            // Create and initialize application context
            ApplicationContext context = new ApplicationContext(cmd);
            context.initialize();
            
            // Execute the requested action
            context.executeAction();
            
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Fatal error: %s", e.getMessage()));
            System.exit(1);
        }
    }
}