/*
 * Copyright 2012-2024 the original author or authors.
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

import com.crunchydata.controller.ColumnController;
import com.crunchydata.controller.TableController;
import com.crunchydata.controller.ReconcileController;
import com.crunchydata.controller.RepoController;
import com.crunchydata.model.DCTable;
import com.crunchydata.model.DCTableMap;
import com.crunchydata.services.*;
import com.crunchydata.util.Logging;
import com.crunchydata.util.Settings;

import org.apache.commons.cli.*;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.crunchydata.controller.TableController.getTableMap;
import static com.crunchydata.util.Settings.Props;
import static com.crunchydata.util.Settings.setProjectConfig;

/**
 * @author Brian Pace
 */
public class pgCompare {
    private static final String THREAD_NAME = "main";

    private static String action = "reconcile";
    private static Integer batchParameter;
    private static boolean check;
    private static CommandLine cmd;
    private static boolean mapOnly;
    private static Integer pid = 1;
    private static Connection connRepo;
    private static Connection connSource;
    private static long startStopWatch;
    private static Connection connTarget;

    public static void main(String[] args) {

        // Catch Shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> Logging.write("info", THREAD_NAME, "Shutting down")));

        // Capture the start time for the compare run.
        startStopWatch = System.currentTimeMillis();

        // Command Line Options
        cmd = parseCommandLine(args);
        if (cmd == null) return;

        // Process Startup
        Logging.write("info", THREAD_NAME,  String.format("Starting - rid: %s", startStopWatch));
        Logging.write("info", THREAD_NAME, String.format("Version: %s",Settings.VERSION));
        Logging.write("info", THREAD_NAME, String.format("Batch Number: %s",batchParameter));
        Logging.write("info", THREAD_NAME, String.format("Recheck Out of Sync: %s",check));

        // Preflight
        preflight.database("source");
        preflight.database("target");

        // Connect to Repository
        Logging.write("info", THREAD_NAME, "Connecting to repository database");
        connRepo = dbPostgres.getConnection(Props, "repo", THREAD_NAME);
        if (connRepo == null) {
            Logging.write("severe", THREAD_NAME, "Cannot connect to repository database");
            System.exit(1);
        }

        // Load Properties from Project (dc_project)
        setProjectConfig(connRepo, pid, Props);

        // Output parameter settings
        Logging.write("info", THREAD_NAME, "Parameters: ");

        Props.entrySet().stream()
                .filter(e -> !e.getKey().toString().contains("password"))
                .forEach(e -> Logging.write("info", THREAD_NAME, String.format("  %s",e)));

        // Initialize pgCompare repository if the action is init.
        // After initialization, exit.
        if ("init".equals(action)) {
            dbRepository.createRepository(connRepo);
            try {
                connRepo.close();
            } catch (Exception e) {
                Logging.write("severe", THREAD_NAME, String.format("Error closing connection to repository: %s",e.getMessage()));
            }
            System.exit(0);
        }


        // Connect to Source
        Logging.write("info", THREAD_NAME, "Connecting to source database");
        connSource = getDatabaseConnection(Props.getProperty("source-type"), "source");

        if (connSource == null) {
            Logging.write("severe", THREAD_NAME, "Cannot connect to source database");
            System.exit(1);
        }

        // Connect to Target
        Logging.write("info", THREAD_NAME, "Connecting to target database");
        connTarget = getDatabaseConnection(Props.getProperty("target-type"), "target");

        if (connTarget == null) {
            Logging.write("severe", THREAD_NAME, "Cannot connect to target database");
            System.exit(1);
        }

        // Refresh Column Map (maponly)
        if (cmd.hasOption("maponly")) {
            // Discover Columns
            ColumnController.discoverColumns(pid, connRepo, connSource, connTarget);
            System.exit(0);
        }

        // Call module/function to perform desired action
        switch (action) {
            case "discovery":
                performDiscovery();
                break;
            case "reconcile":
                performReconciliation();
                break;
            default:
                Logging.write("severe", THREAD_NAME, "Invalid action specified");
                showHelp();
                System.exit(1);
        }

        try {
            connRepo.close();
        } catch (Exception e) {
            // do nothing
        }
        try {
            connTarget.close();
        } catch (Exception e) {
            // do nothing
        }
        try {
            connSource.close();
        } catch (Exception e) {
            // do nothing
        }

    }

    //
    // Database Connection
    //
    private static Connection getDatabaseConnection(String dbType, String destType) {
        return switch (dbType) {
            case "oracle" -> dbOracle.getConnection(Props, destType);
            case "mysql" -> dbMySQL.getConnection(Props, destType);
            case "mssql" -> dbMSSQL.getConnection(Props, destType);
            case "db2" -> dbDB2.getConnection(Props, destType);
            default -> dbPostgres.getConnection(Props, destType, THREAD_NAME);
        };
    }

    //
    // Discovery
    //
    private static void performDiscovery() {
        String sourceSchema = (cmd.hasOption("discovery")) ? cmd.getOptionValue("discovery") : (System.getenv("PGCOMPARE-DISCOVERY") == null) ? "" : System.getenv("PGCOMPARE-DISCOVERY");
        String targetSchema = (cmd.hasOption("discovery")) ? cmd.getOptionValue("discovery") : (System.getenv("PGCOMPARE-DISCOVERY") == null) ? "" : System.getenv("PGCOMPARE-DISCOVERY");

        Logging.write("info", THREAD_NAME, "Performaning table discovery");

        // Discover Tables
        TableController.discoverTables(pid,connRepo,connSource,connTarget,sourceSchema,targetSchema);

        // Discover Columns
        ColumnController.discoverColumns(pid, connRepo, connSource, connTarget);
    }

    //
    // Reconciliation
    //
    private static void performReconciliation () {
        String table = (cmd.hasOption("table")) ? cmd.getOptionValue("table") : "";
        RepoController rpc = new RepoController();
        int tablesProcessed = 0;
        CachedRowSet crsTable = rpc.getTables(pid, connRepo, batchParameter, table, check);
        JSONArray runResult = new JSONArray();

        try {
            while (crsTable.next()) {
                tablesProcessed++;

                // Construct DCTable Class
                DCTable dct = new DCTable();
                dct.setPid(pid);
                dct.setTid(crsTable.getInt("tid"));
                dct.setStatus(crsTable.getString("status"));
                dct.setBatchNbr(crsTable.getInt("batch_nbr"));
                dct.setParallelDegree(crsTable.getInt("parallel_degree"));
                dct.setTableAlias(crsTable.getString("table_alias"));

                Logging.write("info", THREAD_NAME, "Start reconciliation");

                // Construct DCTableMap Class for Source
                DCTableMap sourceTableMap = createTableMap("source",dct);

                // Construct DCTableMap Class for Target
                DCTableMap targetTableMap = createTableMap("target",dct);

                // Create Table History Entry
                rpc.startTableHistory(connRepo,dct.getTid(), "reconcile", dct.getBatchNbr());

                // Clear previous reconciliation results if not recheck.
                if (!check) {
                    Logging.write("info", THREAD_NAME, "Clearing data compare findings");
                    rpc.deleteDataCompare(connRepo, dct.getTid(), dct.getBatchNbr());
                }

                JSONObject actionResult = ReconcileController.reconcileData(connRepo, connSource, connTarget, startStopWatch, check, dct, sourceTableMap, targetTableMap);

                rpc.completeTableHistory(connRepo, dct.getTid(), "reconcile", dct.getBatchNbr(), 0, actionResult.toString());

                runResult.put(actionResult);

            }

            crsTable.close();

        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error performing data reconciliation: %s",e.getMessage()));
        }

        if (!mapOnly) {
            printSummary(tablesProcessed, runResult, startStopWatch);
        }

    }

    //
    // Create Table Map
    //
    private static DCTableMap createTableMap(String tableOrigin, DCTable dct) {
        DCTableMap dctm = getTableMap(connRepo, dct.getTid(),tableOrigin);
        dctm.setBatchNbr(dct.getBatchNbr());
        dctm.setPid(pid);
        dctm.setTableAlias(dct.getTableAlias());

        return dctm;
    }

    //
    // Command Line Options
    //
    private static CommandLine parseCommandLine(String[] args) {
        Options options = new Options();

        options.addOption(Option.builder("b").longOpt("batch").argName("batch").hasArg(true).desc("Batch Number").build());
        options.addOption(Option.builder("c").longOpt("check").argName("check").hasArg(false).desc("Recheck out of sync rows").build());
        options.addOption(Option.builder("d").longOpt("discovery").argName("discovery").hasArg(false).desc("Discover tables in database").build());
        options.addOption(Option.builder("h").longOpt("help").argName("help").hasArg(false).desc("Usage and help").build());
        options.addOption(Option.builder("i").longOpt("init").argName("init").hasArg(false).desc("Initialize repository").build());
        options.addOption(Option.builder("m").longOpt("maponly").argName("maponly").hasArg(false).desc("Perform column mapping only").build());
        options.addOption(Option.builder("p").longOpt("project").argName("project").hasArg(true).desc("Project ID").build());
        options.addOption(Option.builder("t").longOpt("table").argName("table").hasArg(true).desc("Limit to specified table").build());
        options.addOption(Option.builder("v").longOpt("version").argName("version").hasArg(false).desc("Version").build());

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help")) {
                showHelp();
                return null;
            }
            if (cmd.hasOption("version")) {
                showVersion();
                return null;
            }

            if (cmd.hasOption("project")) {
                pid = Integer.parseInt(cmd.getOptionValue("project"));
            }

            // Capture Argument Values
            batchParameter = (cmd.hasOption("batch")) ? Integer.parseInt(cmd.getOptionValue("batch")) : (System.getenv("PGCOMPARE-BATCH") == null) ? 0 : Integer.parseInt(System.getenv("PGCOMPARE-BATCH"));
            check = cmd.hasOption("check");
            mapOnly = cmd.hasOption("maponly");

            // Determine the desired action, reconcile, discovery or init.
            //   reconcile = Perform comparison between source and target databases.
            //   discovery = Perform table discovery on source and target.
            //   init      = Initilize the pgCompare repository.
            action = (cmd.hasOption("discovery")) ? "discovery" : action;
            action = (cmd.hasOption("init")) ? "init" : action;

            return cmd;
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            showHelp();
            return null;
        }
    }

    //
    // Print Summary
    //
    private static void printSummary(int tablesProcessed, JSONArray runResult, long startStopWatch) {
        Logging.write("info", "main", String.format("Processed %s tables",tablesProcessed));
        long endStopWatch = System.currentTimeMillis();
        long totalRows = 0;
        long outOfSyncRows = 0;
        DecimalFormat df = new DecimalFormat("###,###,###,###,###");

        for (int i = 0; i < runResult.length(); i++) {
            JSONObject result = runResult.getJSONObject(i);
            totalRows += result.getInt("equal") + result.getInt("notEqual") + result.getInt("missingSource") + result.getInt("missingTarget");
            outOfSyncRows += result.getInt("notEqual") + result.getInt("missingSource") + result.getInt("missingTarget");
            String msgFormat = "Table Summary: Table = %-30s; Status = %-12s; Equal = %19.19s; Not Equal = %19.19s; Missing Source = %19.19s; Missing Target = %19.19s";
            Logging.write("info", "main", String.format(msgFormat, result.getString("tableName"),
                    result.getString("compareStatus"), df.format(result.getInt("equal")),
                    df.format(result.getInt("notEqual")), df.format(result.getInt("missingSource")),
                    df.format(result.getInt("missingTarget"))));
        }

        String msgFormat = "Run Summary:  Elapsed Time (seconds) = %s; Total Rows Processed = %s; Total Out-of-Sync = %s; Through-put (rows/per second) = %s";
        Logging.write("info", "main", String.format(msgFormat, df.format((endStopWatch - startStopWatch) / 1000),
                df.format(totalRows), df.format(outOfSyncRows), df.format(totalRows / ((endStopWatch - startStopWatch) / 1000))));
    }


    //
    // Help
    //
    public static void showHelp () {
        System.out.println();
        System.out.println("Options:");
        System.out.println("   -b|--batch <batch nbr>");
        System.out.println("   -c|--check Check out of sync rows");
        System.out.println("   -d|--discovery <schema> Discover tables in database");
        System.out.println("   -m|--maponly Only perform column mapping");
        System.out.println("   -p|--project Projrect ID");
        System.out.println("   -t|--table <target table>");
        System.out.println("   --help");
        System.out.println();
    }

    //
    // Version
    //
    public static void showVersion () {
        System.out.println();
        System.out.printf("Version: %s%n",Settings.VERSION);
        System.out.println();
    }


}
