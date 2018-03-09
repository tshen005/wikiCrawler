package edu.ucr.cs242.indexing;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

public class IndexThread extends Thread {
    /**
     * The number of records to be batch-read per SQL transaction.
     */
    public static final int BATCH_READ_COUNT = 50;
    /**
     * The SQL query statement.
     */
    public static final String SQL_QUERY = "SELECT title, content, categories FROM pages LIMIT ? OFFSET ?";

    private final Indexer indexer;
    private final int threadId;
    private final int pageStartIndex;
    private final int numOfPages;
    private final Connection dbConnection;
    private final IndexWriter indexWriter;

    /**
     * Consturct an indexing thread with given settings.
     * @param indexer        The associated indexer.
     * @param threadId       The associated thread id.
     * @param pageStartIndex The page offset (in the database) to start indexing.
     * @param numOfPages     The number of pages to index.
     * @param dbConnection   The connection to the database.
     * @param indexWriter    The index writer.
     */
    public IndexThread(Indexer indexer, int threadId, int pageStartIndex, int numOfPages,
                       Connection dbConnection, IndexWriter indexWriter) {
        this.indexer = indexer;
        this.threadId = threadId;
        this.pageStartIndex = pageStartIndex;
        this.numOfPages = numOfPages;
        this.dbConnection = dbConnection;
        this.indexWriter = indexWriter;
    }

    @Override
    public void run() {
        int indexedCount = 0;

        System.out.println("IndexerThread " + threadId + " started at " + LocalDateTime.now().toLocalTime() + ". " +
                "Pages to index: " + numOfPages + ".");
        while (indexedCount < numOfPages) {
            int localCount = 0;

            try (PreparedStatement statement = dbConnection.prepareStatement(SQL_QUERY)) {
                statement.setInt(1, Math.min(BATCH_READ_COUNT, numOfPages - indexedCount));
                statement.setInt(2, pageStartIndex + indexedCount);

                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        String title = result.getString("title");
                        String content = result.getString("content");
                        String categories = result.getString("categories");

                        Document doc = new Document();
                        doc.add(new Field("title", title, TextField.TYPE_STORED));
                        // Content & categories are indexed only, but not stored,
                        // to save the space. We'll fetch the content from our database.
                        doc.add(new Field("content", content, TextField.TYPE_NOT_STORED));
                        doc.add(new Field("categories", categories, TextField.TYPE_NOT_STORED));
                        indexWriter.addDocument(doc);

                        ++localCount;
                    }
                }

                indexWriter.commit();

                indexedCount += localCount;
                indexer.reportProgress(localCount);
            } catch (SQLException e) {
                System.out.println("IndexerThread " + threadId + " throws an SQLException.");
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("IndexerThread " + threadId + " throws an IOException.");
                e.printStackTrace();
            }
        }
    }
}
