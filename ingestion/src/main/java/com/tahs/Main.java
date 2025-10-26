package com.tahs;

import io.javalin.Javalin;
import io.javalin.http.Context;

import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.tahs.infrastructure.FsDatalakeRepository;
import com.tahs.application.ports.DatalakeRepository;
import com.tahs.application.usecase.IngestionService;

public class Main {

    private static final String STAGING_PATH = "staging/downloads";
    private static final String DATALAKE_PATH = "datalake";
    private static final int TOTAL_BOOKS = 70000;
    private static final int MAX_RETRIES = 10;
    private static final int PORT = 7070;

    private static IngestionService ingestionService;

    public static void main(String[] args) {
        System.out.println("[MAIN] Booting ingestion service + HTTP API...");

        try {
            Files.createDirectories(Paths.get(STAGING_PATH));
            Files.createDirectories(Paths.get(DATALAKE_PATH));
        } catch (Exception e) {
            System.err.println("[ERROR] Could not create required directories: " + e.getMessage());
            return;
        }

        DatalakeRepository datalakeRepo = new FsDatalakeRepository(DATALAKE_PATH);
        ingestionService = new IngestionService(datalakeRepo, Paths.get(STAGING_PATH), TOTAL_BOOKS, MAX_RETRIES);

        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json").start(PORT);

        app.post("/ingest/{book_id}", Main::downloadBook);
        app.get("/ingest/status/{book_id}", Main::checkStatus);
        app.get("/ingest/list", Main::listBooks);

        System.out.println("[API] Ingestion API running on http://localhost:" + PORT + "/");
    }

    private static void downloadBook(Context ctx) {
        int bookId = Integer.parseInt(ctx.pathParam("book_id"));
        System.out.println("[API] Received ingestion request for book " + bookId);

        boolean ok = ingestionService.downloadBookToStaging(bookId);
        if (!ok) {
            ctx.status(400).json(Map.of(
                    "book_id", bookId,
                    "status", "failed",
                    "message", "Download failed or invalid book"
            ));
            return;
        }

        boolean datalakeOk = ingestionService.moveToDatalake(bookId, LocalDateTime.now());
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
        String path = ingestionService.relativePathFor(bookId, LocalDateTime.now());

        ctx.json(Map.of(
                "book_id", bookId,
                "status", "downloaded",
                "path", path,
                "date", date,
                "hour", hour
        ));
    }

    private static void checkStatus(Context ctx) {
        int bookId = Integer.parseInt(ctx.pathParam("book_id"));
        boolean exists = ingestionService.existsInDatalake(bookId);

        ctx.json(Map.of(
                "book_id", bookId,
                "status", exists ? "available" : "not_found"
        ));
    }

    private static void listBooks(Context ctx) {
        var books = ingestionService.listBooks();
        ctx.json(Map.of(
                "count", books.size(),
                "books", books
        ));
    }
}
