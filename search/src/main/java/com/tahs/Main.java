package com.tahs;

import com.mongodb.client.MongoClients;
import com.tahs.application.dto.SearchDto;
import com.tahs.application.usecase.QueryBooksUseCase;
import com.tahs.infrastructure.persistence.MongoInvertedIndexRepository;
import com.tahs.infrastructure.persistence.MongoMetadataRepository;
import io.javalin.Javalin;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        createApp().start(9090);
        System.out.println("Query Engine started on http://localhost:9090");
    }

    private static Javalin createApp() {
        var mongoClient = MongoClients.create("mongodb://localhost:27017");
        var indexService = new MongoInvertedIndexRepository(mongoClient, "books", "inverted_index");
        var metadataRepository = new MongoMetadataRepository(mongoClient, "books", "metadata");
        var queryUseCase = new QueryBooksUseCase(indexService,metadataRepository);

        Javalin app = Javalin.create(config -> config.http.defaultContentType = "application/json");

        app.get("/search", ctx -> {
            try {
                Set<String> allowedParams = Set.of("q","author", "language", "year");
                Map<String, List<String>> filteredParams =ctx.queryParamMap().entrySet().stream()
                        .filter(e -> allowedParams.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                var term = filteredParams.get("q").stream().findFirst().orElse(null);
                if (term == null || term.trim().isEmpty()) {
                    ctx.status(400).json(new ErrorResponse("Query parameter 'q' is required"));
                    return;
                }
                SearchDto results = queryUseCase.execute(filteredParams);
                ctx.json(results);
            } catch (Exception e) {
                ctx.status(500).json(new ErrorResponse("Search error: " + e.getMessage()));
                e.printStackTrace();
            }
        });
        return app;
    }

    public static class ErrorResponse {
        public String error;

        public ErrorResponse(String error) {
            this.error = error;
        }
    }
}
