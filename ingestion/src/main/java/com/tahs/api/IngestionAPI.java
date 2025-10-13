package com.tahs.api;

import io.javalin.Javalin;
import io.javalin.http.Context;
import com.tahs.BookFunctions;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class IngestionAPI {

    private static final String STAGING_PATH = "../staging/downloads";
    private static final String DATALAKE_PATH = "../datalake";

    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(7070);

        app.post("/ingest/{book_id}", com.tahs.api.IngestionAPI::downloadBook);
        app.get("/ingest/status/{book_id}", com.tahs.api.IngestionAPI::checkStatus);
        app.get("/ingest/list", com.tahs.api.IngestionAPI::listBooks);

        System.out.println("[API] Ingestion API running on http://localhost:7070/");
    }

    private static void downloadBook(Context ctx) {
        int bookId = Integer.parseInt(ctx.pathParam("book_id"));
        System.out.println("[API] Received ingestion request for book " + bookId);

        boolean ok = BookFunctions.downloadBook(bookId, STAGING_PATH);
        if (!ok) {
            ctx.status(400).json(Map.of(
                    "book_id", bookId,
                    "status", "failed",
                    "message", "Download failed or invalid book"
            ));
            return;
        }

        boolean datalakeOk = BookFunctions.createDatalake(bookId, STAGING_PATH);
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
                    .filter(p -> p.getFileName().toString().endsWith(".body.txt"))
                    .map(p -> p.getFileName().toString().split("\\.")[0])
                    .map(Integer::parseInt)
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
