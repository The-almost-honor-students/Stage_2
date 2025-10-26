package com.tahs.application.usecase;

import com.tahs.IngestionFunctions;
import io.javalin.http.Context;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IngestionService {

    private static final String STAGING_PATH = "../staging/downloads";
    private static final String DATALAKE_PATH = "../datalake";

    public static void downloadBook(Context ctx) {
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

    public static void checkStatus(Context ctx) {
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

    public static void listBooks(Context ctx) {
        try {
            List<Integer> books = Files.walk(Paths.get(DATALAKE_PATH))
                    .filter(Files::isRegularFile)
                    .map(p -> p.getFileName().toString())
                    .filter(name -> name.endsWith(".body.txt"))
                    .map(name -> name.split("\\.")[0])
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
