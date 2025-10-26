package com.tahs;

import io.javalin.Javalin;
import io.javalin.http.Context;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Main {

    private static final Path CONTROL_PATH = Paths.get("../control");
    private static final Path DOWNLOADS_FILE = CONTROL_PATH.resolve("downloaded_books.txt");
    private static final String STAGING_PATH = "../staging/downloads";
    private static final String DATALAKE_PATH = "../datalake";

    private static final int TOTAL_BOOKS = 70000;
    private static final int MAX_RETRIES = 10;
    private static final int PORT = 7070;

    private static final Random random = new Random();

    public static void main(String[] args) {
        System.out.println("[MAIN] Booting ingestion service + HTTP API...");

        try {
            Files.createDirectories(CONTROL_PATH);
            Files.createDirectories(Paths.get(STAGING_PATH));
            Files.createDirectories(Paths.get(DATALAKE_PATH));
        } catch (IOException e) {
            System.err.println("[ERROR] Could not create required directories: " + e.getMessage());
            return;
        }

        Javalin app = Javalin.create(cfg -> {
            cfg.http.defaultContentType = "application/json";
        }).start(PORT);

        app.post("/ingest/{book_id}", Main::downloadBook);
        app.get("/ingest/status/{book_id}", Main::checkStatus);
        app.get("/ingest/list", Main::listBooks);

        System.out.println("[API] Ingestion API running on http://localhost:" + PORT + "/");

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(Main::downloadCycle, 0, 1, TimeUnit.SECONDS);
        System.out.println("[INGESTION] Continuous download cycle scheduled (1 book/second).");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[MAIN] Shutdown requested. Stopping scheduler and API...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException ignored) {
                scheduler.shutdownNow();
            }
            app.stop();
            System.out.println("[MAIN] Stopped.");
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("[MAIN] Interrupted, exiting...");
        }
    }

    private static void downloadBook(Context ctx) {
        int bookId = Integer.parseInt(ctx.pathParam("book_id"));
        System.out.println("[API] Received ingestion request for book " + bookId);

        boolean ok = IngestionFunctions.downloadBook(bookId, STAGING_PATH);
        if (!ok) {
            ctx.status(400).json(Map.of(
                    "book_id", bookId,
                    "status", "failed",
                    "message", "Download failed or invalid book"
            ));
            return;
        }

        boolean datalakeOk = IngestionFunctions.createDatalake(bookId, STAGING_PATH);
        if (!datalakeOk) {
            ctx.status(500).json(Map.of(
                    "book_id", bookId,
                    "status", "failed",
                    "message", "Failed to move files to datalake"
            ));
            return;
        }

        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String hour = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH"));
        String path = String.format("datalake/%s/%s/%d", date, hour, bookId);

        ctx.json(Map.of(
                "book_id", bookId,
                "status", "downloaded",
                "path", path
        ));
    }

    private static void checkStatus(Context ctx) {
        int bookId = Integer.parseInt(ctx.pathParam("book_id"));
        try {
            boolean exists = Files.walk(Paths.get(DATALAKE_PATH))
                    .filter(Files::isRegularFile)
                    .anyMatch(p -> p.getFileName().toString().startsWith(bookId + ".body.txt"));

            ctx.json(Map.of(
                    "book_id", bookId,
                    "status", exists ? "available" : "not_found"
            ));
        } catch (IOException e) {
            ctx.status(500).json(Map.of("error", e.getMessage()));
        }
    }

    private static void listBooks(Context ctx) {
        try {
            List<Integer> books = Files.walk(Paths.get(DATALAKE_PATH))
                    .filter(Files::isRegularFile)
                    .map(p -> p.getFileName().toString())
                    .filter(name -> name.endsWith(".body.txt"))
                    .map(name -> name.split("\\.")[0])
                    .map(Integer::parseInt)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());

            ctx.json(Map.of(
                    "count", books.size(),
                    "books", books
            ));
        } catch (IOException e) {
            ctx.status(500).json(Map.of("error", e.getMessage()));
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
                boolean ok = IngestionFunctions.downloadBook(candidateId, STAGING_PATH);
                if (ok) {
                    boolean datalakeOk = IngestionFunctions.createDatalake(candidateId, STAGING_PATH);
                    if (datalakeOk) {
                        appendDownloadedId(candidateId);
                        System.out.println("[CONTROL] Book " + candidateId + " downloaded and moved to datalake successfully.");
                        return;
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
