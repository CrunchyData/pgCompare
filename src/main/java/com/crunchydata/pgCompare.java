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
import java.util.List;

import static com.crunchydata.controller.TableController.getTableMap;
import static com.crunchydata.services.Reporter.createSection;
import static com.crunchydata.services.Reporter.generateHtmlReport;
import static com.crunchydata.util.Settings.*;

import com.crunchydata.controller.ColumnController;
import com.crunchydata.controller.TableController;
import com.crunchydata.controller.ReconcileController;
import com.crunchydata.controller.RepoController;
import com.crunchydata.models.DCTable;
import com.crunchydata.models.DCTableMap;
import com.crunchydata.services.*;
import com.crunchydata.util.Logging;
import com.crunchydata.util.Settings;

import org.apache.commons.cli.*;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * @author Brian Pace
 */
public class pgCompare {
    private static final String THREAD_NAME = "main";

    private static String action = "reconcile";
    private static String reportFileName;
    private static Integer batchParameter;
    private static boolean check, genReport;
    private static CommandLine cmd;
    private static Integer pid = 1;
    private static Connection connRepo;
    private static Connection connSource;
    private static long startStopWatch;
    private static Connection connTarget;

    public static void main(String[] args) {

        // Command Line Options
        cmd = parseCommandLine(args);
        if (cmd == null) return;

        Logging.initialize(Props);

        // Catch Shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> Logging.write("info", THREAD_NAME, "Shutting down")));

        // Capture the start time for the compare run.
        startStopWatch = System.currentTimeMillis();

        // Process Startup
        Logging.write("info", THREAD_NAME,  String.format("Starting - rid: %s", startStopWatch));
        Logging.write("info", THREAD_NAME, String.format("Version: %s",Settings.VERSION));
        Logging.write("info", THREAD_NAME, String.format("Batch Number: %s",batchParameter));
        Logging.write("info", THREAD_NAME, String.format("Recheck Out of Sync: %s",check));

        // Connect to Repository
        Logging.write("info", THREAD_NAME, "Connecting to repository database");
        connRepo = dbPostgres.getConnection(Props, "repo", THREAD_NAME);
        if (connRepo == null) {
            Logging.write("severe", THREAD_NAME, "Cannot connect to repository database");
            System.exit(1);
        }

        // Load Properties from Project (dc_project)
        if ( !action.equals("init") ) {
            setProjectConfig(connRepo, pid);
        }

        // Preflight
        preflight.database(Props,"source");
        preflight.database(Props,"target");

        // Sort and Output parameter settings
        Logging.write("info", THREAD_NAME, "Parameters: ");

        Props.entrySet().stream()
                .filter(e -> !e.getKey().toString().contains("password"))
                .sorted((e1, e2) -> e1.getKey().toString().compareTo(e2.getKey().toString()))
                .forEach(e -> Logging.write("info", THREAD_NAME, String.format("  %s",e)));

        // Initialize pgCompare repository if the action is init.
        // After initialization, exit.
        if ("init".equals(action)) {
            dbRepository.createRepository(Props, connRepo);
            try {
                connRepo.close();
            } catch (Exception e) {
                Logging.write("severe", THREAD_NAME, String.format("Error closing connection to repository: %s",e.getMessage()));
            }
            System.exit(0);
        }

        // Connect to Source
        connSource = getDatabaseConnection(Props.getProperty("source-type"), "source");

        // Connect to Target
        connTarget = getDatabaseConnection(Props.getProperty("target-type"), "target");

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

        closeDatabaseConnection(connRepo);
        closeDatabaseConnection(connTarget);
        closeDatabaseConnection(connSource);

    }

    //
    // Database Connection
    //
    private static void closeDatabaseConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                Logging.write("warning", THREAD_NAME, "Error closing connection: " + e.getMessage());
            }
        }
    }

    private static Connection getDatabaseConnection(String dbType, String destRole) {
        Logging.write("info", THREAD_NAME, String.format("(%s) Connecting to database (type = %s, host = %s)", destRole, Props.getProperty(destRole+"-type"), Props.getProperty(destRole+"-host")));

        Connection conn = switch (dbType) {
            case "oracle" -> dbOracle.getConnection(Props, destRole);
            case "mariadb" -> dbMariaDB.getConnection(Props, destRole);
            case "mysql" -> dbMySQL.getConnection(Props, destRole);
            case "mssql" -> dbMSSQL.getConnection(Props, destRole);
            case "db2" -> dbDB2.getConnection(Props, destRole);
            default -> dbPostgres.getConnection(Props, destRole, THREAD_NAME);
        };

        if (conn == null) {
            Logging.write("severe", THREAD_NAME, String.format("Cannot connect to %s database", destRole));
            System.exit(1);
        }

        return conn;
    }

    //
    // Discovery
    //
    private static void performDiscovery() {
        Logging.write("info", THREAD_NAME, "Performing table discovery");
        String table = (cmd.hasOption("table")) ? cmd.getOptionValue("table").toLowerCase() : "";

        // Discover Tables
        TableController.discoverTables(Props, pid, table, connRepo,connSource,connTarget);

        // Discover Columns
        ColumnController.discoverColumns(Props, pid, table, connRepo, connSource, connTarget);
    }

    //
    // Reconciliation
    //
    private static void performReconciliation () {
        String table = (cmd.hasOption("table")) ? cmd.getOptionValue("table").toLowerCase() : "";
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

                Logging.write("info", THREAD_NAME, String.format("--- START RECONCILIATION FOR TABLE:  %s ---",dct.getTableAlias().toUpperCase()));

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

                JSONObject actionResult = ReconcileController.reconcileData(Props, connRepo, connSource, connTarget, startStopWatch, check, dct, sourceTableMap, targetTableMap);

                rpc.completeTableHistory(connRepo, dct.getTid(), "reconcile", dct.getBatchNbr(), 0, actionResult.toString());

                runResult.put(actionResult);

            }

            crsTable.close();

        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error performing data reconciliation: %s",e.getMessage()));
        }

        createSummary(tablesProcessed, runResult, startStopWatch);

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
        options.addOption(Option.builder("p").longOpt("project").argName("project").hasArg(true).desc("Project ID").build());
        options.addOption(Option.builder("r").longOpt("report").argName("report").hasArg(true).desc("Generate report").build());
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
            genReport = cmd.hasOption("report");
            reportFileName = (cmd.hasOption("report")) ? cmd.getOptionValue("report") : "";

            // Determine the desired action, reconcile, discovery or init.
            //   reconcile = Perform comparison between source and target databases.
            //   discovery = Perform table discovery on source and target.
            //   init      = Initialize the pgCompare repository.
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
    // Create Column Metadata JSON Object
    //
    private static JSONObject createReportColumn(String header, String key, String align, boolean commaFormat) {
        return new JSONObject()
                .put("columnHeader", header)
                .put("columnClass", align)
                .put("columnKey", key)
                .put("commaFormat", commaFormat);
    }

    //
    // Create Summary
    //
    private static void createSummary(int tablesProcessed, JSONArray runResult, long startStopWatch) {
        printSummary("Summary: ",0);

        if ( tablesProcessed > 0 ) {
            long endStopWatch = System.currentTimeMillis();
            long totalRows = 0;
            long outOfSyncRows = 0;
            long elapsedTime = (endStopWatch - startStopWatch) / 1000;
            // Ensure elapsed time is at least 1 second.
            elapsedTime = elapsedTime == 0 ? 1 : elapsedTime;

            DecimalFormat df = new DecimalFormat("###,###,###,###,###");

            // Iterate through the runResult array and compute totals
            for (int i = 0; i < runResult.length(); i++) {
                JSONObject result = runResult.getJSONObject(i);
                int nbrEqual = result.getInt("equal");
                int notEqual = result.getInt("notEqual");
                int missingSource = result.getInt("missingSource");
                int missingTarget = result.getInt("missingTarget");

                totalRows += nbrEqual + notEqual + missingSource + missingTarget;
                outOfSyncRows += notEqual + missingSource + missingTarget;

                // Print per table summary
                printSummary(String.format("TABLE: %s", result.getString("tableName")), 4);
                printSummary(String.format("Table Summary: Status         = %s", result.getString("compareStatus")), 8);
                printSummary(String.format("Table Summary: Equal          = %19d", nbrEqual), 8);
                printSummary(String.format("Table Summary: Not Equal      = %19d", notEqual), 8);
                printSummary(String.format("Table Summary: Missing Source = %19d", missingSource), 8);
                printSummary(String.format("Table Summary: Missing Target = %19d", missingTarget), 8);
            }

            // Print job summary
            printSummary("Job Summary: ", 0);
            printSummary(String.format("Tables Processed               = %s", tablesProcessed), 2);
            printSummary(String.format("Elapsed Time (seconds)         = %s", df.format(elapsedTime)), 2);
            printSummary(String.format("Total Rows Processed           = %s", df.format(totalRows)), 2);
            printSummary(String.format("Total Out-of-Sync              = %s", df.format(outOfSyncRows)), 2);
            printSummary(String.format("Through-put (rows/per second)  = %s", df.format(totalRows / elapsedTime)), 2);

            // Generate Report
            if (genReport) {
                // Create JSON report
                JSONObject jobSummary = new JSONObject()
                        .put("tablesProcessed", df.format(tablesProcessed))
                        .put("elapsedTime", df.format(elapsedTime))
                        .put("totalRows", df.format(totalRows))
                        .put("outOfSyncRows", df.format(outOfSyncRows))
                        .put("rowsPerSecond", df.format(totalRows / elapsedTime));


                JSONArray jobSummaryLayout = new JSONArray(List.of(
                        createReportColumn("Tables Processed", "tablesProcessed", "right-align", false),
                        createReportColumn("Elapsed Time", "elapsedTime", "right-align", false),
                        createReportColumn("Rows per Second", "rowsPerSecond", "right-align", false),
                        createReportColumn("Total Rows", "totalRows", "right-align", false),
                        createReportColumn("Out of Sync Rows", "outOfSyncRows", "right-align", false)
                ));


                JSONArray runResultLayout = new JSONArray(List.of(
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

                JSONArray reportArray = new JSONArray()
                        .put(createSection("Job Summary", new JSONArray().put(jobSummary), jobSummaryLayout)) // Pass JSONObject directly
                        .put(createSection("Table Summary", runResult, runResultLayout)); // Pass runResult

                if (check) {
                    JSONArray runCheckResultLayout = new JSONArray(List.of(
                            createReportColumn("Primary Key", "pk", "left-align", false),
                            createReportColumn("Status", "compareStatus", "left-align", false),
                            createReportColumn("Result", "compareResult", "left-align", false)
                    ));

                    for (int i =0; i<runResult.length(); i++ ) {
                        reportArray.put(createSection(String.format("Table: %s", runResult.getJSONObject(i).getString("tableName")), runResult.getJSONObject(i).getJSONObject("checkResult").getJSONArray("data"), runCheckResultLayout));
                    }
                }

                generateHtmlReport(reportArray, reportFileName,"pgCompare Summary");

            }

        } else {
            // Print message if no tables processed or out of sync records found
            String msg = (check) ? "No out of sync records found" : "No tables were processed. Need to do discovery? Used correct batch nbr?";
            Logging.write("warning", THREAD_NAME, msg);
        }
    }

    //
    // Print Summary
    //
    private static void printSummary(String message, int indent) {
        Logging.write("info", "summary", " ".repeat(indent) + message);
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
        System.out.println("   -i|--init <schema> Init tables in database");
        System.out.println("   -p|--project Project ID");
        System.out.println("   -r|--report <file> Create html report of compare");
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
