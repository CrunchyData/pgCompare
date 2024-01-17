/*
 * Copyright 2012-2023 the original author or authors.
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

import java.sql.Connection;
import javax.sql.rowset.CachedRowSet;
import java.text.DecimalFormat;
import java.util.Map;

import com.crunchydata.controller.ReconcileController;
import com.crunchydata.controller.RepoController;
import com.crunchydata.services.*;
import com.crunchydata.util.Logging;
import com.crunchydata.util.Settings;

import org.apache.commons.cli.*;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.crunchydata.util.Settings.Props;

/**
 * @author Brian Pace
 */
public class ConferoDC {

    public static void main(String[] args) {

        /////////////////////////////////////////////////
        // Command Line Options
        /////////////////////////////////////////////////
        long startStopWatch = System.currentTimeMillis();
        Options options = new Options();

        options.addOption(Option.builder("b")
                .longOpt("batch")
                .argName("batch")
                .hasArg(true)
                .desc("Batch Number")
                .build());

        options.addOption(Option.builder("c")
                .longOpt("check")
                .argName("check")
                .hasArg(false)
                .desc("Recheck out of sync rows")
                .build());

        options.addOption(Option.builder("h")
                .longOpt("help")
                .argName("help")
                .hasArg(false)
                .desc("Usage and help")
                .build());

        options.addOption(Option.builder("t")
                .longOpt("table")
                .argName("table")
                .hasArg(true)
                .desc("Limit to specified table")
                .build());

        options.addOption(Option.builder("v")
                .longOpt("version")
                .argName("version")
                .hasArg(false)
                .desc("Version")
                .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            showHelp();
            System.exit(1);
        }

        if ( cmd.hasOption("help")) {
            showHelp();
            System.exit(0);
        }

        if ( cmd.hasOption("version")) {
            showVersion();
            System.exit(0);
        }


        // Capture Argument Values
        Integer batchParameter = (cmd.hasOption("batch")) ? Integer.parseInt(cmd.getOptionValue("batch")) : (System.getenv("CONFERODC-BATCH") == null ) ? 0 : Integer.parseInt(System.getenv("CONFERODC-BATCH"));
        boolean check = cmd.hasOption("check");
        String table = (cmd.hasOption("table")) ? cmd.getOptionValue("table") : "" ;

        /////////////////////////////////////////////////
        // Process Startup
        /////////////////////////////////////////////////
        Logging.write("info", "main", "Starting - rid: " + startStopWatch);
        Logging.write("info", "main", "Version: "+ Settings.version);
        Logging.write("info", "main", "Batch Number: "+ batchParameter);
        Logging.write("info", "main", "Recheck Out of Sync: "+ check);

        /////////////////////////////////////////////////
        // Catch Shutdown
        /////////////////////////////////////////////////
        Runtime.getRuntime().addShutdownHook(new Thread(() -> Logging.write("info", "main", "Shutting down")));

        /////////////////////////////////////////////////
        // Show Parameters
        /////////////////////////////////////////////////
        Logging.write("info", "main", "Parameters: ");

        for(Map.Entry<Object, Object> e : Props.entrySet()) {
            if (e.getKey().toString().contains("password")) {
                Logging.write("info", "main", "  password: ********");
            } else {
                Logging.write("info", "main", "  "+e);
            }
        }

        /////////////////////////////////////////////////
        // Connect to Repository
        /////////////////////////////////////////////////
        Connection repoConn;
        Logging.write("info", "main", "Connecting to repository database");
        repoConn = dbPostgres.getConnection(Props,"repo", "main");
        if ( repoConn == null) {
            Logging.write("severe", "main", "Cannot connect to repository database");
            System.exit(1);
        }

        /////////////////////////////////////////////////
        // Connect to Source
        /////////////////////////////////////////////////
        Connection sourceConn;
        Logging.write("info", "main", "Connecting to source database");
        if (Props.getProperty("source-type").equals("oracle")) {
            sourceConn = dbOracle.getConnection(Props,"source");
        } else {
            sourceConn = dbPostgres.getConnection(Props,"source", "main");
        }
        if ( sourceConn == null) {
            Logging.write("severe", "main", "Cannot connect to source database");
            System.exit(1);
        }

        /////////////////////////////////////////////////
        // Connect to Target
        /////////////////////////////////////////////////
        Connection targetConn;
        Logging.write("info", "main", "Connecting to target database");
        if (Props.getProperty("target-type").equals("oracle")) {
            targetConn = dbOracle.getConnection(Props,"target");
        } else {
            targetConn = dbPostgres.getConnection(Props,"target", "main");
        }
        if ( targetConn == null) {
            Logging.write("severe", "main", "Cannot connect to target database");
            System.exit(1);
        }

        /////////////////////////////////////////////////
        // Data Reconciliation
        /////////////////////////////////////////////////
        RepoController rpc = new RepoController();
        int tablesProcessed = 0;
        CachedRowSet crsTable = rpc.getTables(repoConn, batchParameter, table, check);

        JSONObject actionResult;
        JSONArray runResult = new JSONArray();

        try {
            while (crsTable.next()) {
                tablesProcessed++;

                Logging.write("info", "main", "Start reconciliation");
                rpc.startTableHistory(repoConn,crsTable.getInt("tid"),"reconcile",crsTable.getInt("batch_nbr"));
                ////////////////////////////////////////
                // Prepare Data Compare Table
                ////////////////////////////////////////

                if (!check) {
                    Logging.write("info", "main", "Clearing data compare findings");
                    rpc.deleteDataCompare(repoConn, "source", crsTable.getString("source_table"), crsTable.getInt("batch_nbr"));
                    rpc.deleteDataCompare(repoConn, "target", crsTable.getString("target_table"), crsTable.getInt("batch_nbr"));
                }

                actionResult = ReconcileController.reconcileData(repoConn,
                        sourceConn,
                        targetConn,
                        crsTable.getString("source_schema"), crsTable.getString("source_table"),
                        crsTable.getString("target_schema"), crsTable.getString("target_table"),
                        crsTable.getString("table_filter"),
                        crsTable.getString("mod_column"),
                        crsTable.getInt("parallel_degree"),
                        startStopWatch,
                        check,
                        crsTable.getInt("batch_nbr"),
                        crsTable.getInt("tid"));
                rpc.completeTableHistory(repoConn, crsTable.getInt("tid"), "reconcile", crsTable.getInt("batch_nbr"), 0, actionResult.toString());

                runResult.put(actionResult);

            }

            crsTable.close();

        } catch (Exception e) {
            Logging.write("severe", "main", "Error performing data reconciliation: " + e.getMessage());
        }

        try { repoConn.close(); } catch (Exception e) {
            // do nothing
        }
        try { targetConn.close(); } catch (Exception e) {
            // do nothing
        }
        try { sourceConn.close(); } catch (Exception e) {
            // do nothing
        }

        /////////////////////////////////////////////////
        // Print Summary
        /////////////////////////////////////////////////
        Logging.write("info", "main", "Processed " + tablesProcessed + " tables");
        long endStopWatch = System.currentTimeMillis();
        long totalRows = 0;
        long outofsyncRows = 0;
        String msgFormat;
        DecimalFormat df = new DecimalFormat("###,###,###,###,###");

        for (int i = 0; i < runResult.length(); i++) {
            totalRows += runResult.getJSONObject(i).getInt("equal")+runResult.getJSONObject(i).getInt("notEqual")+runResult.getJSONObject(i).getInt("missingSource")+runResult.getJSONObject(i).getInt("missingTarget");
            outofsyncRows += runResult.getJSONObject(i).getInt("notEqual")+runResult.getJSONObject(i).getInt("missingSource")+runResult.getJSONObject(i).getInt("missingTarget");
            msgFormat = "Table Summary: Table = %-30s; Status = %-12s; Equal = %19.19s; Not Equal = %19.19s; Missing Source = %19.19s; Missing Target = %19.19s";
            Logging.write("info", "main", String.format(msgFormat,runResult.getJSONObject(i).getString("tableName"),
                                                                                  runResult.getJSONObject(i).getString("compareStatus"),
                                                                                  df.format(runResult.getJSONObject(i).getInt("equal")),
                                                                                  df.format(runResult.getJSONObject(i).getInt("notEqual")),
                                                                                  df.format(runResult.getJSONObject(i).getInt("missingSource")),
                                                                                  df.format(runResult.getJSONObject(i).getInt("missingTarget"))));
        }
        msgFormat = "Run Summary:  Elapsed Time (seconds) = %s; Total Rows Processed = %s; Total Out-of-Sync = %s; Through-put (rows/per second) = %s";
        Logging.write("info", "main", String.format(msgFormat,df.format((endStopWatch-startStopWatch)/1000),df.format(totalRows), df.format(outofsyncRows), df.format( totalRows/((endStopWatch-startStopWatch)/1000) ) ));
    }

    /////////////////////////////////////////////////
    // Help
    /////////////////////////////////////////////////
    public static void showHelp () {
        System.out.println();
        System.out.println("Options:");
        System.out.println("   -b|--batch <batch nbr>");
        System.out.println("   -c|--check Check out of sync rows");
        System.out.println("   -t|--table <target table>");
        System.out.println("   --help");
        System.out.println();
    }

    /////////////////////////////////////////////////
    // Version
    /////////////////////////////////////////////////
    public static void showVersion () {
        System.out.println();
        System.out.println("Version: "+ Settings.version);
        System.out.println();
    }


}
