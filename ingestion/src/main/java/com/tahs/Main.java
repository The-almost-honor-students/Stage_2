package com.tahs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    private static final Path CONTROL_PATH = Paths.get("../control");
    private static final Path DOWNLOADS_FILE = CONTROL_PATH.resolve("downloaded_books.txt");
    private static final String STAGING_PATH = "../staging/downloads";
    private static final int TOTAL_BOOKS = 70000;
    private static final int MAX_RETRIES = 10;

    private static final Random random = new Random();

    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.out.println("[INGESTION] Starting continuous download cycle (1 book/second)...");

        try {
            Files.createDirectories(CONTROL_PATH);
            Files.createDirectories(Paths.get(STAGING_PATH));
        } catch (IOException e) {
            System.out.println("[ERROR] Could not create required directories: " + e.getMessage());
            return;
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try {
            scheduler.scheduleAtFixedRate(Main::downloadCycle, 0, 1, TimeUnit.SECONDS);

            // Keep the program running
            Thread.currentThread().join();

        } catch (InterruptedException e) {
            System.out.println("[CONTROL] Interrupted, shutting down scheduler...");

        } finally {
            // Ensure the scheduler is always shut down
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException ex) {
                scheduler.shutdownNow();
            }
            System.out.println("[CONTROL] Scheduler stopped.");
        }
    }

    private static void downloadCycle() {
        try {
            Set<String> downloaded = readDownloadedIds();

            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                int candidateId = random.nextInt(TOTAL_BOOKS) + 1;
                if (downloaded.contains(String.valueOf(candidateId))) {
                    System.out.println("[INFO] Book " + candidateId + " already downloaded, looking for another one...");
                    continue;
                }

                System.out.println("[CONTROL] Downloading new book with ID " + candidateId + "...");
                boolean ok = BookFunctions.downloadBook(candidateId, STAGING_PATH);
                if (ok) {
                    boolean datalakeOk = BookFunctions.createDatalake(candidateId, STAGING_PATH);
                    if (datalakeOk) {
                        appendDownloadedId(candidateId);
                        System.out.println("[CONTROL] Book " + candidateId + " downloaded and moved to datalake successfully.");
                        return; // End this cycle; the next one starts in 1 second
                    }
                }
            }

            System.out.println("[CONTROL] No valid new book found in this cycle.");

        } catch (Exception e) {
            System.out.println("[ERROR] Failure during download cycle: " + e.getMessage());
        }
    }

    private static Set<String> readDownloadedIds() throws IOException {
        Set<String> ids = new HashSet<>();
        if (Files.exists(DOWNLOADS_FILE)) {
            ids.addAll(Files.readAllLines(DOWNLOADS_FILE, StandardCharsets.UTF_8));
        }
        return ids;
    }

    private static void appendDownloadedId(int id) {
        try {
            Files.createDirectories(CONTROL_PATH);
            Files.writeString(DOWNLOADS_FILE, id + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("[WARN] Could not register downloaded ID " + id + ": " + e.getMessage());
        }
    }
}
