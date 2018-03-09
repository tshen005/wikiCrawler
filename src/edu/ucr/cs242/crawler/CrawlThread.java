package edu.ucr.cs242.crawler;

import edu.ucr.cs242.OnThreadExitEventListener;
import edu.ucr.cs242.Utility;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The actual thread for crawling, also a producer class.
 */
public class CrawlThread extends Thread {
    private final int threadId;
    private Set<String> visitedUrls;
    private final int numOfPages;
    private final int crawlDepth;
    private final int crawlInterval;
    private final String entryUrl;
    private final String crawlHostRegex;
    private final String crawlPathRegex;
    private final RobotPolicy robotPolicy;

    private class QueueItem {
        private final String url;
        private final int depth;

        public String getUrl() {
            return url;
        }

        public int getDepth() {
            return depth;
        }

        public QueueItem(String url, int depth) {
            this.url = url;
            this.depth = depth;
        }
    }

    private int crawlCount = 0;
    private final Queue<QueueItem> nextUrlQueue = new LinkedList<>();

    private final BlockingQueue<WikiPage> pageQueue = new LinkedBlockingQueue<>();
    private final WriterThread writer;

    /**
     * Construct a crawler thread with given settings.
     *
     * @param threadId       The associated thread id.
     * @param visitedUrls    The set of visited urls; the underlying object should be thread-safe.
     * @param numOfPages     The number of web pages to crawl.
     * @param crawlDepth     The depth of web pages to crawl.
     * @param crawlInterval  The interval of crawling next page, limiting the access rate (milliseconds).
     * @param entryUrl       The url of the entry page.
     * @param crawlHostRegex The url to be crawled should be within this host.
     * @param crawlPathRegex The path of the url should start with this prefix.
     * @param jdbcUrl        The JDBC url to access database.
     * @param robotPolicy    The policy the crawler should obey.
     */
    public CrawlThread(int threadId, Set<String> visitedUrls,
                       int numOfPages, int crawlDepth, int crawlInterval,
                       String entryUrl, String crawlHostRegex, String crawlPathRegex,
                       String jdbcUrl, RobotPolicy robotPolicy) throws SQLException {
        this.threadId = threadId;
        this.visitedUrls = visitedUrls;
        this.numOfPages = numOfPages;
        this.crawlDepth = crawlDepth;
        this.crawlInterval = crawlInterval;
        this.entryUrl = entryUrl;
        this.crawlHostRegex = crawlHostRegex;
        this.crawlPathRegex = crawlPathRegex;
        this.writer = new WriterThread(threadId, jdbcUrl, pageQueue);
        this.robotPolicy = robotPolicy;
    }

    /**
     * Set the event listener for WriterThread's exiting.
     * @param exitEventListener The event listener.
     */
    public void setWriterExitListener(OnThreadExitEventListener exitEventListener) {
        writer.setExitEventListener(exitEventListener);
    }

    private void process(QueueItem nextUrl) {
        Document doc = null;
        URL actualUrl;

        try {
            doc = Jsoup.connect(nextUrl.getUrl()).get();
            // Since Special:Random returns 302, the actual url should be parsed after redirect.
            actualUrl = new URL(doc.location());
        } catch (IOException e) {
            System.out.println("CrawlThread " + threadId + " throws an IOException: " + e.getMessage());
            if (e instanceof MalformedURLException && doc != null) {
                System.out.println("CrawlThread " + threadId + " reports a malformed URL: " + doc.location());
            }
            return;
        }

        // The redirected url may a special page, filter them out first.
        if (actualUrl.getHost().matches(crawlHostRegex) && actualUrl.getPath().matches(crawlPathRegex)) {
            // Remove the anchor part.
            visitedUrls.add(actualUrl.getProtocol() + "://" + actualUrl.getHost() + actualUrl.getFile());

            Element elTitle = doc.getElementById("firstHeading"); // key
            Element elContent = doc.selectFirst("#mw-content-text .mw-parser-output"); // value 1
            Element elCategory = doc.getElementById("mw-normal-catlinks"); // value 2
            Element elLastMod = doc.getElementById("footer-info-lastmod");

            if (elTitle != null && elContent != null && elCategory != null) {
                String title = elTitle.text().trim();

                // Remove all reference <sup>s.
                elContent.select("sup[class='reference']").remove();
                // Remove the `edit` links.
                elContent.select("span[class='mw-editsection']").remove();
                // Remove unused tags (table & div).
                Arrays.asList("table", "div").forEach(tag -> elContent.select(tag).remove());
                // Remove empty headings with no paragraphs below it.
                Arrays.asList("h1", "h2", "h3", "h4", "h5", "h6").forEach(
                        tag -> elContent.select(tag + "+" + tag).stream()
                                .map(Element::previousElementSibling)
                                .forEach(Element::remove));
                // The final content can be now generated.
                String content = elContent.children().stream()
                        // We don't need empty elements (that is with no text).
                        .filter(Element::hasText)
                        // Map to its un-encoded text & trim
                        .map(Element::wholeText).map(String::trim)
                        // Collect back to a full string
                        .collect(Collectors.joining("\n"));

                // For categories, we want the text in `#mw-normal-catlinks ul > li`
                List<String> categories = elCategory.select("ul > li").stream()
                        .map(Element::text)
                        .map(String::trim)
                        .collect(Collectors.toList());

                // The last modification timestamp is stored in the 2nd <script> tag from the bottom.
                // If not found, use current date time as the last modification.
                LocalDateTime lastModify = elLastMod == null ? LocalDateTime.now() : Stream.of(elLastMod)
                        // Something like "This page was last edited on 18 January 2018, at 21:30."
                        .map(el -> {
                            Pattern pattern = Pattern.compile("edited on ([^,]*), at ([^.]*)");
                            Matcher matcher = pattern.matcher(el.html());
                            return matcher.find() && matcher.groupCount() == 2 ?
                                    matcher.group(1) + " " + matcher.group(2) : null;
                        }).filter(Objects::nonNull)
                        // It is in a format of 2 January 2018, at 21:30.
                        .map(time -> LocalDateTime.parse(time,
                                DateTimeFormatter.ofPattern("d MMMM yyyy HH:mm", Locale.US)))
                        // If not found, use current date time as the last modification.
                        .findFirst().orElse(LocalDateTime.now());

                // We won't store empty page.
                if (content.isEmpty() || categories.isEmpty())
                    return;

                // Get all valid `#mw-content-text > a`s.
                // We want <a> with attribute of href.
                Supplier<Stream<URL>> linkSupplier = () -> elContent.select("a[href]").stream()
                        .map(a -> a.attr("href"))
                        // Map href into URL object
                        .map(href -> {
                            try { return new URL(actualUrl, href); }
                            catch (MalformedURLException e) { return null; }
                        }).filter(Objects::nonNull)
                        // We only want the link inside a given host and the path meets some requirement.
                        .filter(url -> url.getHost().matches(crawlHostRegex) && url.getPath().matches(crawlPathRegex));

                // Save all the outgoing titles.
                List<String> outLinks = linkSupplier.get().map(URL::getPath)
                        // Decode URL to UTF-8 first.
                        .map(url -> {
                            try { return URLDecoder.decode(url, "UTF-8"); }
                            catch (UnsupportedEncodingException e) { return null; }
                        }).filter(Objects::nonNull)
                        // crawlPathRegex has built-in group, to fetch the title.
                        .map(url -> {
                            Pattern pattern = Pattern.compile(crawlPathRegex);
                            Matcher matcher = pattern.matcher(url);
                            return matcher.find() && matcher.groupCount() == 1 ? matcher.group(1) : null;
                        }).filter(Objects::nonNull)
                        // We save titles, thus replace all _ in the link to space.
                        .map(dest -> dest.replace('_', ' '))
                        .distinct().collect(Collectors.toList());

                // Put into writing queue
                try { pageQueue.put(new WikiPage(title, content, categories, lastModify, outLinks)); }
                // Oops! Something wrong...
                catch (InterruptedException e) { return; }

                // Update the crawled pages count.
                ++crawlCount;

                // Hit the depth limit?
                if (nextUrl.getDepth() >= crawlDepth)
                    return;

                // Reconstruct the URL, remove the anchor part.
                // There may be some duplicate URLs after this processing.
                linkSupplier.get().map(url -> url.getProtocol() + "://" + url.getHost() + url.getFile())
                        // Check if the URL has already stored in the stack.
                        .distinct().filter(url -> !visitedUrls.contains(url))
                        // Push into queue.
                        .forEachOrdered(url -> nextUrlQueue.add(new QueueItem(url, nextUrl.getDepth() + 1)));
            }
        }
    }

    private void reportProgress(boolean summary, LocalDateTime startAt) {
        System.out.format("%sCrawlThread %d crawled %d pages, %.2f%% completed. Elapsed time: %s.%n",
                summary ? "Summary: " : "", threadId, crawlCount, crawlCount * 100.0f / numOfPages,
                Utility.elapsedTime(startAt, LocalDateTime.now()));
    }

    @Override
    public void run() {
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("CrawlThread " + threadId + " started at " + startAt.toLocalTime() + ". " +
                "Pages to crawl: " + numOfPages + ".");

        writer.start();
        nextUrlQueue.add(new QueueItem(entryUrl, 0));

        // Job finished? or something wrong with writer?
        while (crawlCount < numOfPages && !writer.isInterrupted()) {
            // nextUrlQueue may be empty, since the crawl depth limitation.
            // If so, crawl the entry url again. (Entry url is never put into visitedUrls)
            QueueItem nextUrl = nextUrlQueue.isEmpty() ? new QueueItem(entryUrl, 0) : nextUrlQueue.remove();

            // Check if url is restricted by some policies.
            try {
                if (!robotPolicy.testURL(new URL(nextUrl.getUrl()))) {
                    // Entry url? No need to run the crawler.
                    if (nextUrl.getUrl().equals(entryUrl)) {
                        System.out.println("CrawlThread " + threadId + " reported the entry url (" +
                                entryUrl + ") is disallowed. Exiting...");
                        break;
                    } else {
                        continue;
                    }
                }
            } catch (MalformedURLException e) {
                // ignored
            }

            if (!visitedUrls.contains(nextUrl.getUrl())) {
                process(nextUrl);

                // Report crawling progress.
                if (crawlCount > 0 && crawlCount % Math.min(numOfPages, WriterThread.BATCH_WRITE_COUNT) == 0) {
                    reportProgress(false, startAt);
                }

                // Be polite.
                try { Thread.sleep(crawlInterval); }
                // We don't care if it is interrupted
                catch (InterruptedException e) { }
            }
        }

        // Check if writer has been interrupted (mostly due to exception).
        // If not, we have to stop writer after the pageQueue is processed.
        if (!writer.isInterrupted()) {
            try {
                synchronized (pageQueue) {
                    while (!pageQueue.isEmpty())
                        pageQueue.wait();
                }
            } catch (InterruptedException e) {
                // Actions are performed in finally block.
            } finally {
                writer.interrupt();
                // Wait WriterThread to exit.
                try { writer.join(); }
                // Who the f**k always interrupts us?
                catch (InterruptedException e) { }
            }

            reportProgress(true, startAt);
        }
    }
}