package edu.ucr.cs242.crawler;

import edu.ucr.cs242.OnThreadExitEventListener;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * The consumer class, to write data into database.
 */
public class WriterThread extends Thread {
    /**
     * The number of records to be batch-written per SQL transaction.
     */
    public static final int BATCH_WRITE_COUNT = 50;
    /**
     * The SQL insert statement.
     */
    public static final String SQL_INSERT =
            "INSERT OR IGNORE INTO pages (title, content, categories, lastModify, outLinks) VALUES (?, ?, ?, ?, ?)";

    private final int threadId;
    private final BlockingQueue<WikiPage> pageQueue;

    private Connection dbConnection;
    private OnThreadExitEventListener exitEventListener;

    public void setExitEventListener(OnThreadExitEventListener exitEventListener) {
        this.exitEventListener = exitEventListener;
    }

    /**
     * Construct a writer thread, with given settings.
     * @param threadId  The associated thread id.
     * @param jdbcUrl   The JDBC connection string.
     * @param pageQueue The producer-consumer queue.
     * @throws SQLException
     */
    public WriterThread(int threadId, String jdbcUrl, BlockingQueue<WikiPage> pageQueue) throws SQLException {
        this.threadId = threadId;
        this.pageQueue = pageQueue;

        this.dbConnection = DriverManager.getConnection(jdbcUrl);
        this.dbConnection.setAutoCommit(false);
    }

    @Override
    public void run() {
        int bufferedCount = 0;
        int committedCount = 0;

        System.out.println("WriterThread " + threadId + " started at " + LocalDateTime.now().toLocalTime() + ".");
        try (PreparedStatement statement = dbConnection.prepareStatement(SQL_INSERT)) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    WikiPage page = pageQueue.take();

                    statement.setString(1, page.getTitle());
                    statement.setString(2, page.getContent());
                    statement.setString(3, page.getCategories().stream().collect(Collectors.joining("|")));
                    statement.setString(4, page.getLastModify().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")));
                    statement.setString(5, page.getOutLinks().stream().collect(Collectors.joining("|")));
                    statement.addBatch();

                    if (++bufferedCount % BATCH_WRITE_COUNT == 0) {
                        int sum = Arrays.stream(statement.executeBatch()).sum();
                        dbConnection.commit();
                        committedCount += sum;

                        System.out.format("WriterThread %d committed %d pages. Most recent one: %s.%n",
                                threadId, sum, page.getTitle());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // The final commit.
            int sum = Arrays.stream(statement.executeBatch()).sum();
            dbConnection.commit();
            committedCount += sum;
        } catch (Exception e) {
            System.out.println("WriterThread " + threadId + " throws an exception.");
            e.printStackTrace();

            // Something wrong, we have to rollback the transaction.
            Thread.currentThread().interrupt();
            try { dbConnection.rollback(); }
            catch (SQLException _e) { _e.printStackTrace(); }
        } finally {
            try { dbConnection.close(); }
            catch (SQLException _e) { _e.printStackTrace(); }

            System.out.format("Summary: WriterThread %d committed %d pages in total.%n", threadId, committedCount);

            // Normal exit?
            if (!Thread.currentThread().isInterrupted()) {
                synchronized (pageQueue) {
                    pageQueue.notify();
                }
            }

            if (exitEventListener != null) {
                exitEventListener.onExitEvent(committedCount);
            }
        }
    }
}
