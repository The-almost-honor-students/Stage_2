package com.tahs.orchestrator;

import com.tahs.clients.IndexingClient;
import com.tahs.clients.IngestionClient;
import com.tahs.clients.SearchClient;
import com.tahs.tracker.DownloadTracker;
import com.tahs.tracker.IndexingTracker;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class Orchestrator {
    private final IngestionClient ingestionClient;
    private final IndexingClient indexingClient;
    private final SearchClient searchClient;
    private static final int MAX_BOOKS = 70_000;
    private static final long MAX_RETRIES = 10;

    public Orchestrator(IngestionClient ingestionClient,
                        IndexingClient indexingClient,
                        SearchClient searchClient) {
        this.ingestionClient = ingestionClient;
        this.indexingClient = indexingClient;
        this.searchClient = searchClient;
    }

    public void execute() throws IOException, InterruptedException {
        DownloadTracker.createFileIfNotExists();
        IndexingTracker.createFileIfNotExists();
        if (areMissingBooksIndexed()) {
            var indexedBooks = IndexingTracker.getIndexedBooks();
            var downloadedBooks = DownloadTracker.getDownloadedBooks();
            Set<String> indexedSet = new HashSet<>(indexedBooks);

            List<String> notIndexedBooks = downloadedBooks.stream()
                    .filter(id -> !indexedSet.contains(id))
                    .collect(Collectors.toList());
            var bookId = getBookId(notIndexedBooks);
            var response = indexingClient.updateIndexForBook(bookId);
            if (response.statusCode() == 200) {
                IndexingTracker.markAsIndexed(bookId);
            } else {
                throw new IOException("Fail with Status: " + response.statusCode() +
                        " and body: " + response.body());
            }
        } else {
            var bookId = String.valueOf(ThreadLocalRandom.current().nextInt(1, MAX_BOOKS + 1));
            if (!DownloadTracker.isDownloaded(bookId)) {
                ingestionClient.downloadBook(bookId);
                if (checkBookIsDownloaded(bookId)) DownloadTracker.markAsDownloaded(bookId);
            }
        }
    }

    private static String getBookId(List<String> notIndexedBooks) {
        return notIndexedBooks.stream().findFirst().get();
    }

    public SearchClient getSearchClient() {
        return this.searchClient;
    }

    private static boolean areMissingBooksIndexed() {
        return DownloadTracker.getDownloadedBooks().size() - IndexingTracker.getIndexedBooks().size() > 0;
    }

    private boolean checkBookIsDownloaded(String bookId) throws IOException, InterruptedException {
        var retry = 0;
        while (retry < MAX_RETRIES) {
            var response = this.ingestionClient.status(bookId);
            if (response.statusCode() == 200) {
                System.out.println("Book downloaded.");
                return true;
            }
            retry++;
        }
        return false;
    }
}