package com.tahs;

import io.javalin.Javalin;
import io.javalin.http.Context;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import com.tahs.application.usecase.IngestionService;

public class Main {

    private static final String STAGING_PATH = "staging/downloads";
    private static final String DATALAKE_PATH = "datalake";

    private static final int PORT = 7070;


    public static void main(String[] args) {
        System.out.println("[MAIN] Booting ingestion service + HTTP API...");

        try {
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

    }

    private static void downloadBook(Context ctx) {
        int bookId = Integer.parseInt(ctx.pathParam("book_id"));
        System.out.println("[API] Received ingestion request for book " + bookId);

        boolean ok = IngestionService.downloadBook(bookId, STAGING_PATH);
        if (!ok) {
            ctx.status(400).json(Map.of(
                    "book_id", bookId,
                    "status", "failed",
                    "message", "Download failed or invalid book"
            ));
            return;
        }

        boolean datalakeOk = IngestionService.createDatalake(bookId, STAGING_PATH);
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

}
