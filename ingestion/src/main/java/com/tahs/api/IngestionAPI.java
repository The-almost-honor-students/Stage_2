package com.tahs.api;

import com.tahs.application.usecase.IngestionService;
import io.javalin.Javalin;

public class IngestionAPI {

    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(7070);

        app.post("/ingest/{book_id}", IngestionService::downloadBook);
        app.get("/ingest/status/{book_id}", IngestionService::checkStatus);
        app.get("/ingest/list", IngestionService::listBooks);

        System.out.println("[API] Ingestion API running on http://localhost:7070/");
    }
}
