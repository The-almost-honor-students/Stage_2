package com.tahs.orchestrator;

import com.tahs.clients.IndexingClient;
import com.tahs.clients.IngestionClient;
import com.tahs.clients.SearchClient;
import com.tahs.tracker.DownloadTracker;
import com.tahs.tracker.IndexingTracker;

import java.io.IOException;

public class Orchestrator {
    private final IngestionClient ingestionClient;
    private final IndexingClient indexingClient;
    private final SearchClient searchClient;

    private static final long MAX_TIMEOUT = System.currentTimeMillis() + 60 * 1000L;
    private static final int DOWNLOAD_POLL_INTERVAL_MS = 500;

    public Orchestrator(IngestionClient ingestionClient,
                        IndexingClient indexingClient,
                        SearchClient searchClient) {
        this.ingestionClient = ingestionClient;
        this.indexingClient = indexingClient;
        this.searchClient = searchClient;
    }

    public void execute(String bookId) throws IOException, InterruptedException {
        DownloadTracker.createFileIfNotExists();
        IndexingTracker.createFileIfNotExists();

        if (areMissingBooksIndexed()) {
            var response = indexingClient.updateIndexForBook(bookId);
            if (response.statusCode() == 200) {
                IndexingTracker.markAsIndexed(bookId);
            } else {
                throw new IOException("Fail with Status: " + response.statusCode() +
                        " and body: " + response.body());
            }
        } else {
            if (!DownloadTracker.isDownloaded(bookId)) {
                ingestionClient.downloadBook(bookId);
                if (checkBookIsDownloaded(bookId)) DownloadTracker.markAsDownloaded(bookId);
            }
        }
    }

    public SearchClient getSearchClient() {
        return this.searchClient;
    }

    private static boolean areMissingBooksIndexed() {
        return DownloadTracker.countDownloadedBooks() - IndexingTracker.countIndexedBooks() > 0;
    }

    private boolean checkBookIsDownloaded(String bookId) throws IOException, InterruptedException {
        while (System.currentTimeMillis() < MAX_TIMEOUT) {
            var response = this.ingestionClient.status(bookId);
            if (response.statusCode() == 200 && isDownloadedResponse(response.body())) {
                System.out.println("Book downloaded.");
                return true;
            }
            Thread.sleep(DOWNLOAD_POLL_INTERVAL_MS);
        }
        return false;
    }

    private static boolean isDownloadedResponse(String body) {
        if (body == null) return false;
        String s = body.toLowerCase();
        return s.contains("\"downloaded\":true")
                || s.contains("\"ready\":true")
                || s.contains("\"status\":\"downloaded\"")
                || s.contains("\"state\":\"downloaded\"");
    }
}