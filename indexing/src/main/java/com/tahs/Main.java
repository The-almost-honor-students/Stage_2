package com.tahs;

import com.tahs.application.usecase.IndexService;
import com.tahs.infrastructure.persistence.MongoInvertedIndexRepository;
import com.tahs.infrastructure.persistence.MongoMetadataRepository;
import io.javalin.Javalin;
import com.google.gson.Gson;


import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String[] args) {
        createApp().start(8080);

    }

    public static Javalin createApp() {
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(8080);
        Gson gson = new Gson();

        app.get("/index/status", ctx -> {
            Map<String, Object> status = Map.of(
                    "books_indexed", 1200,
                    "last_update", "2025-10-08T14:05:00Z",
                    "index_size_MB", 42.7
            );
            ctx.result(gson.toJson(status));
        });

        app.post("/index/update/{book_id}", ctx -> {
            String bookId = ctx.pathParam("book_id");
            System.out.println("Indexing book " + bookId + "...");
            MongoInvertedIndexRepository indexRepository = new MongoInvertedIndexRepository();
            MongoMetadataRepository metadataRepository = new MongoMetadataRepository();
            var indexService = new IndexService(indexRepository, metadataRepository);
            indexService.updateByBookId(bookId);

            // TODO: Implement update index logic
            // ...
            //...
            //...
            Map<String, Object> response = Map.of(
                    "book_id", bookId,
                    "index", "updated"
            );
            ctx.result(gson.toJson(response));
        });

        app.post("/index/rebuild", ctx -> {
            String bookId = ctx.pathParam("book_id");

            System.out.println("Indexing book " + bookId + "...");

            Map<String, Object> response = Map.of(
                    "books_processed", bookId,
                    "elapsed_time", "35.2s"
            );

            ctx.result(gson.toJson(response));
        });

        return app;
    }
}
