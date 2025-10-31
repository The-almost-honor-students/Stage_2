package com.tahs;

import com.mongodb.client.MongoClients;
import com.tahs.application.dto.SearchDto;
import com.tahs.application.usecase.QueryBooksUseCase;
import com.tahs.config.AppConfig;
import com.tahs.infrastructure.persistence.MongoInvertedIndexRepository;
import com.tahs.infrastructure.persistence.MongoMetadataRepository;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.Javalin;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        var dotenv = Dotenv.configure()
                .ignoreIfMissing()
                .load();
        var config = CheckEnvVars(dotenv);
        createApp(config).start(config.port());
    }

    private static Javalin createApp(AppConfig appConfig) {
        var mongoClient = MongoClients.create(appConfig.dbUrl());
        var indexService = new MongoInvertedIndexRepository(mongoClient, appConfig.databaseName(), appConfig.collectionIndexName());
        var metadataRepository = new MongoMetadataRepository(mongoClient, appConfig.databaseName(), appConfig.collectionMetadataName());
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

    private static AppConfig CheckEnvVars(Dotenv dotenv) {
        String dbUrl = Optional.ofNullable(dotenv.get("MONGO_URL"))
                .orElse(System.getenv("MONGO_URL"));

        String databaseName = Optional.ofNullable(dotenv.get("DATABASE_NAME"))
                .orElse(System.getenv("DATABASE_NAME"));
        String collectionMetaData  = Optional.ofNullable(dotenv.get("COLLECTION_METADATA"))
                .orElse(System.getenv("COLLECTION_METADATA"));
        String collectionIndex  = Optional.ofNullable(dotenv.get("COLLECTION_INDEX"))
                .orElse(System.getenv("COLLECTION_INDEX"));
        String portStr = Optional.ofNullable(dotenv.get("PORT"))
                .orElse(System.getenv("PORT"));
        int port = portStr != null ? Integer.parseInt(portStr) : 8080;
        return new AppConfig(
                dbUrl,
                databaseName,
                collectionMetaData,
                collectionIndex,
                port
        );
    }
}
