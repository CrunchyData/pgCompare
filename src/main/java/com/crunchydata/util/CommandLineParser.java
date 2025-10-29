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

import com.crunchydata.config.Settings;
import org.apache.commons.cli.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.crunchydata.config.Settings.*;

/**
 * Utility class for parsing command line arguments and handling help/version display.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class CommandLineParser {
    
    // Action constants
    private static final String DEFAULT_ACTION = "compare";
    private static final String DEFAULT_BATCH_ENV_VAR = "PGCOMPARE-BATCH";
    
    /**
     * Parse command line arguments and return a CommandLine object.
     * 
     * @param args Command line arguments
     * @return CommandLine object with parsed options, or null if parsing fails
     */
    public static CommandLine parse(String[] args) {
        Options options = createOptions();
        org.apache.commons.cli.CommandLineParser parser = new DefaultParser();
        
        try {
            // Find the verb first (non-option argument)
            String verb = DEFAULT_ACTION;
            List<String> argList = new ArrayList<>(Arrays.asList(args));
            Iterator<String> iter = argList.iterator();
            while (iter.hasNext()) {
                String current = iter.next();
                if (!current.startsWith("-")) {
                    verb = current;
                    iter.remove(); // Remove the verb so it's not parsed as an unknown option
                    break;
                }
            }

            CommandLine cmd = parser.parse(options, argList.toArray(new String[0]));

            // Handle help/version actions
            if (cmd.hasOption("help")) {
                showHelp();
                return null;
            }
            if (cmd.hasOption("version")) {
                showVersion();
                return null;
            }

            // Set action property
            Props.setProperty("action", verb.toLowerCase());
            
            // Set parameters
            if (cmd.hasOption("project")) {
                Integer pid = Integer.parseInt(cmd.getOptionValue("project"));
                Props.setProperty("pid", pid.toString());
            }

            if (cmd.hasOption("table")) {
                Props.setProperty("table", cmd.getOptionValue("table"));
            }

            Integer batchParameter = (cmd.hasOption("batch")) ?
                    Integer.parseInt(cmd.getOptionValue("batch")) :
                    (System.getenv(DEFAULT_BATCH_ENV_VAR) == null) ? 0 : Integer.parseInt(System.getenv(DEFAULT_BATCH_ENV_VAR));
            
            Props.setProperty("batch", batchParameter.toString());
            Props.setProperty("genReport", Boolean.toString(cmd.hasOption("report")));
            Props.setProperty("reportFileName", cmd.getOptionValue("report", ""));

            return cmd;
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            showHelp();
            return null;
        }
    }
    
    /**
     * Create the command line options.
     * 
     * @return Options object containing all valid options
     */
    private static Options createOptions() {
        Options options = new Options();

        // Define all valid options
        options.addOption(Option.builder("b").longOpt("batch").hasArg().desc("Batch Number").build());
        options.addOption(Option.builder("h").longOpt("help").hasArg(false).desc("Usage and help").build());
        options.addOption(Option.builder("p").longOpt("project").hasArg().desc("Project ID").build());
        options.addOption(Option.builder("r").longOpt("report").hasArg().desc("Generate report").build());
        options.addOption(Option.builder("t").longOpt("table").hasArg().desc("Limit to specified table").build());
        options.addOption(Option.builder("v").longOpt("version").hasArg(false).desc("Version").build());

        return options;
    }

    /**
     * Display help information.
     */
    public static void showHelp() {
        System.out.println();
        System.out.println("pgcompare <action> <options>");
        System.out.println();
        System.out.println("Actions:");
        System.out.println("   check         Recompare the out of sync rows from previous compare");
        System.out.println("   compare       Perform database compare");
        System.out.println("   copy-table    Copy pgCompare metadata for table.  Must specify table alias to copy using --table option");
        System.out.println("   discover      Discover tables and columns");
        System.out.println("   init          Initialize the repository database");
        System.out.println("Options:");
        System.out.println("   -b|--batch <batch nbr>");
        System.out.println("   -p|--project Project ID");
        System.out.println("   -r|--report <file> Create html report of compare");
        System.out.println("   -t|--table <target table>");
        System.out.println("   --help");
        System.out.println();
    }

    /**
     * Display version information.
     */
    public static void showVersion() {
        System.out.println();
        System.out.printf("Version: %s%n", Settings.VERSION);
        System.out.println();
    }
}
