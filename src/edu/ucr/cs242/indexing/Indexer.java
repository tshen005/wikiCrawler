package edu.ucr.cs242.indexing;

import edu.ucr.cs242.Utility;
import org.apache.commons.cli.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class Indexer {
    private final int numOfThreads;
    private final Connection dbConnection;
    private final Path indexOutputPath;
    private final AtomicInteger indexedCount = new AtomicInteger(0);
    private final int numOfPages;
    private LocalDateTime startAt;

    /**
     * Construct an Indexer with given settings.
     * @param numOfThreads    The number of threads for indexing.
     * @param dbConnection    The active database connection.
     * @param indexOutputPath The directory to output the Lucene index.
     */
    public Indexer(int numOfThreads, Connection dbConnection, Path indexOutputPath) {
        this.numOfThreads = numOfThreads;
        this.dbConnection = dbConnection;
        this.indexOutputPath = indexOutputPath;

        numOfPages = fetchPageCount();
        // Check number of pages we have.
        if (numOfPages <= 0) {
            System.out.println("Indexer cannot find any pages to index. Exiting...");
            System.exit(numOfPages);
        }
    }

    private int fetchPageCount() {
        final String SQL_COUNT = "SELECT COUNT(*) FROM pages";
        int numOfPages = -1;

        try (Statement query = dbConnection.createStatement();
             ResultSet result = query.executeQuery(SQL_COUNT)) {

            result.next();
            numOfPages = result.getInt(1);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return numOfPages;
    }

    private void startThreads(IndexWriter indexWriter) {
        IndexThread[] threads = new IndexThread[numOfThreads];

        for (int i = 0, pageStartIndex = 0; i < numOfThreads; i++) {
            int partition = Utility.calculatePartition(numOfPages, numOfThreads, i);
            threads[i] = new IndexThread(this, i, pageStartIndex, partition, dbConnection, indexWriter);
            threads[i].start();
            pageStartIndex += partition;
        }

        Utility.waitThreads(threads);
    }

    /**
     * For thread's invoke of reporting its progress.
     * @param count The count of pages that has been indexed during the last batch-index period.
     */
    public void reportProgress(int count) {
        int after = indexedCount.addAndGet(count);
        if (after == numOfPages || after % 1000 == 0) {
            System.out.format("%sIndexer has indexed %d pages, %.2f%% completed. Elapsed time: %s.%n",
                    after == numOfPages ? "Summary: " : "",
                    after, after * 100.0f / numOfPages, Utility.elapsedTime(startAt, LocalDateTime.now()));
        }
    }

    public void start() {
        try {
            // Create a special analyzer for categories, since they are separated by |.
            CharArraySet categoryStopWords = CharArraySet.copy(StandardAnalyzer.STOP_WORDS_SET);
            categoryStopWords.add("|");

            Map<String, Analyzer> analyzerMap = new HashMap<>();
            analyzerMap.put("categories", new StandardAnalyzer(categoryStopWords));

            // Fallback to StandardAnalyzer, if field is not specified in analyzerMap.
            PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), analyzerMap);
            Directory directory = FSDirectory.open(indexOutputPath);

            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            IndexWriter writer = new IndexWriter(directory, config);

            // Now we can start the indexer.
            startAt = LocalDateTime.now();
            System.out.println("Indexer started at " + startAt.toLocalTime() + ". " +
                    "Pages to index: " + numOfPages + ".");

            startThreads(writer);

            // Some cleanup
            if (writer.hasUncommittedChanges()) {
                writer.commit();
            }
            writer.close();
        } catch (IOException e) {
            System.out.println("Indexer throws an IOException: " + e.getMessage());
        }
    }

    private static void printMessage(String message) {
        System.out.println("indexer: " + message);
    }

    private static void printUsage() {
        System.out.println("usage: indexer [options] <jdbc-url> <index-output-path>");
        System.out.println("use -h for a list of possible options");
        System.exit(1);
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("indexer [options] <jdbc-url> <index-output-path>", options);
        System.out.println();
    }

    private static Optional<Connection> getConnection(String jdbcUrl) throws ClassNotFoundException {
        final String SQL_COUNT = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'pages'";
        Connection dbConnection = null;

        Class.forName("org.sqlite.JDBC");
        try {
            dbConnection = DriverManager.getConnection(jdbcUrl);
            Statement query = dbConnection.createStatement();
            ResultSet result = query.executeQuery(SQL_COUNT);

            result.next();
            int count = result.getInt(1);
            result.close();
            query.close();

            if (count > 0) {
                return Optional.of(dbConnection);
            } else {
                dbConnection.close();
                return Optional.empty();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    public static void main(String args[]) throws SQLException, ClassNotFoundException {
        final int NUMBER_OF_THREADS = 10;

        Options options = new Options();
        options.addOption(Option.builder("t")
                        .longOpt("threads")
                        .argName("NUM OF THREADS")
                        .desc("the number of threads for indexing (default: " + NUMBER_OF_THREADS + ")")
                        .numberOfArgs(1)
                        .build());

        options.addOption(Option.builder("l")
                        .longOpt("log-output")
                        .argName("FILE NAME")
                        .desc("the file to write logs into (default: STDOUT)")
                        .numberOfArgs(1)
                        .build());

        options.addOption("h", "help", false, "print a synopsis of standard options");

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);
            List<String> argList = cmd.getArgList();

            if (cmd.hasOption("h")) {
                printHelp(options);
                System.exit(0);
            }

            if (argList.isEmpty()) {
                printMessage("JDBC url is not specified");
                printUsage();
            }

            if (argList.size() <= 1) {
                printMessage("index output path is not specified");
                printUsage();
            }

            String logOutput = cmd.getOptionValue("log-output");
            if (!Utility.openOutputLog(logOutput)) {
                printMessage("invalid log file path");
                printUsage();
            }

            try {
                int numOfThreads = Integer.parseInt(cmd.getOptionValue("threads", String.valueOf(NUMBER_OF_THREADS)));

                Optional<Connection> dbConnection = getConnection(argList.get(0));
                if (!dbConnection.isPresent()) {
                    printMessage("invalid JDBC url");
                    printUsage();
                } else {
                    Path indexOutputPath = Paths.get(argList.get(1));
                    if (!Files.exists(indexOutputPath) || !Files.isDirectory(indexOutputPath)) {
                        printMessage("invalid index output path (not exist or not directory)");
                        printUsage();
                    }

                    new Indexer(numOfThreads, dbConnection.get(), indexOutputPath).start();
                    dbConnection.get().close();
                }
            } catch (NumberFormatException e) {
                printMessage("invalid option(s)");
                printHelp(options);
                System.exit(1);
            }

        } catch (ParseException e) {
            // Lower the first letter, which as default is an upper letter.
            printMessage(e.getMessage().substring(0, 1).toLowerCase() + e.getMessage().substring(1));
            printHelp(options);
            System.exit(1);
        }
    }
}
