package com.tahs;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tahs.application.exceptions.BookNotFound;
import com.tahs.application.usecase.IndexService;
import com.tahs.infrastructure.persistence.MongoInvertedIndexRepository;
import com.tahs.infrastructure.persistence.MongoMetadataRepository;
import com.tahs.infrastructure.serialization.books.GutenbergHeaderSerializer;
import io.javalin.Javalin;
import com.google.gson.Gson;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) {
        createApp().start(8080);

    }

    public static Javalin createApp() {
        Javalin app = Javalin.create(config -> config.http.defaultContentType = "application/json");
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(Instant.class,
                        (JsonSerializer<Instant>) (src, typeOfSrc, context) ->
                                src == null ? null : new JsonPrimitive(src.toString()))
                .create();

        var mongoClient = MongoClients.create("mongodb://localhost:27017");
        var indexService = getIndexService(mongoClient);

        app.get("/index/status", ctx -> {
            var stats = indexService.getStats();
            ctx.result(gson.toJson(stats));
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

        app.post("/index/update/{book_id}", ctx -> {
            String bookId = ctx.pathParam("book_id");
            System.out.println("Indexing book " + bookId + "...");

            try {
                indexService.updateByBookId(bookId);
                Map<String, Object> response = Map.of(
                        "book_id", bookId,
                        "index", "updated"
                );
                ctx.status(200).result(gson.toJson(response));
            }
            catch (BookNotFound e) {
                ctx.status(404);
                Map<String, Object> error = Map.of(
                        "book_id", bookId,
                        "error", "Book not found",
                        "message", e.getMessage()
                );
                ctx.result(gson.toJson(error));
            }
            catch (Exception e) {
                ctx.status(500);
                Map<String, Object> errorResponse = Map.of(
                        "error", "Error interno al actualizar el índice",
                        "details", e.getMessage()
                );
                ctx.result(gson.toJson(errorResponse));
            }
        });

        app.post("/index/rebuild", ctx -> {
            System.out.println("Rebuild Index ...");
            long start = System.currentTimeMillis();
            indexService.rebuildIndex();
            long finish = System.currentTimeMillis();
            long timeElapsed = finish - start;
            var allBooks = indexService.getAllBooks();
            Map<String, Object> response = Map.of(
                    "books_processed", allBooks.size(),
                    "elapsed_time", TimeUnit.MILLISECONDS.toSeconds(timeElapsed) +"s"
            );
            ctx.result(gson.toJson(response));
        });

        return app;
    }

    @NotNull
    private static IndexService getIndexService(MongoClient mongoClient) {
        var database = "books";
        var collection_metadata = "metadata";
        var collection_index = "inverted_index";

        var indexRepository = new MongoInvertedIndexRepository(mongoClient,database,collection_index);
        var metadataRepository = new MongoMetadataRepository(mongoClient,database,collection_metadata);
        var gutenbergHeaderSerializer = new GutenbergHeaderSerializer();
        return new IndexService(indexRepository, metadataRepository,gutenbergHeaderSerializer);
    }
}
