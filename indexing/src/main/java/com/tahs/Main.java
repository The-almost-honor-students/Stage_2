package com.tahs;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.tahs.application.usecase.IndexService;
import com.tahs.infrastructure.persistence.MongoInvertedIndexRepository;
import com.tahs.infrastructure.persistence.MongoMetadataRepository;
import com.tahs.infrastructure.serialization.books.GutenbergHeaderSerializer;
import io.javalin.Javalin;
import com.google.gson.Gson;
import org.bson.Document;


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
        });
        Gson gson = new Gson();
        var mongoClient = MongoClients.create("mongodb://localhost:27017");
        var database = "books";
        var collection_metadata = "metadata";
        var collection_index = "inverted_index";

        var indexRepository = new MongoInvertedIndexRepository(mongoClient,database,collection_index);
        var metadataRepository = new MongoMetadataRepository(mongoClient,database,collection_metadata);
        var gutenbergHeaderSerializer = new GutenbergHeaderSerializer();
        var indexService = new IndexService(indexRepository, metadataRepository,gutenbergHeaderSerializer);

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
            indexService.updateByBookId(bookId);
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
