package com.tahs;

import io.javalin.Javalin;
import com.google.gson.Gson;


import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final AtomicInteger booksIndexed = new AtomicInteger(0);
    private static LocalDateTime lastUpdate = LocalDateTime.now();
    private static double indexSizeMB = 0.0;

    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(8080);
        Gson gson = new Gson();

        app.get("/status", ctx -> {
            Map<String, String> status = Map.of(
                    "service", "example-service",
                    "status", "running"
            );
            ctx.result(gson.toJson(status));
        });

        app.get("/data", ctx -> {
            Map<String, String> data = Map.of(
                    "service", "example-service",
                    "data", "running"
            );
            ctx.result(gson.toJson(data));
        });

        app.post("/index/update/{book_id}", ctx -> {
            String bookId = ctx.pathParam("book_id");

            System.out.println("Indexing book " + bookId + "...");
            booksIndexed.incrementAndGet();
            lastUpdate = LocalDateTime.now();
            indexSizeMB += Math.random() * 0.5;

            Map<String, Object> response = Map.of(
                    "book_id", bookId,
                    "index", "updated"
            );

            ctx.result(gson.toJson(response));
        });

    }
}
