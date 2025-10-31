package com.tahs;

import com.tahs.clients.IndexingClient;
import com.tahs.clients.IngestionClient;
import com.tahs.clients.SearchClient;
import com.tahs.config.AppConfig;
import com.tahs.orchestrator.Orchestrator;
import io.github.cdimascio.dotenv.Dotenv;

import java.net.http.HttpClient;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;


public class Main {

    public static void main(String[] args) {
        try {
            var dotenv = Dotenv.configure()
                    .ignoreIfMissing()
                    .load();
            var appConfig = CheckEnvVars(dotenv);
            HttpClient httpClient = HttpClient.newHttpClient();
            IngestionClient ingestionClient = new IngestionClient(httpClient, appConfig.urlIngestion());
            IndexingClient indexingClient = new IndexingClient(httpClient, appConfig.urlIndex());
            SearchClient searchClient = new SearchClient(httpClient, appConfig.urlSearch());
            var orchestrator = new Orchestrator(ingestionClient, indexingClient, searchClient);
            var bookId = ThreadLocalRandom.current().nextInt(1, 70001);
            orchestrator.execute(String.valueOf(bookId));
        } catch (Exception e) {
            throw new RuntimeException("Error executing orchestrator", e);
        }

    }
    private static AppConfig CheckEnvVars(Dotenv dotenv) {
        String urlIngestion = Optional.ofNullable(dotenv.get("INGESTION_URL"))
                .orElse(System.getenv("INGESTION_URL"));
        String urlIndexing = Optional.ofNullable(dotenv.get("INDEXING_URL"))
                .orElse(System.getenv("INDEXING_URL"));
        String urlSearch = Optional.ofNullable(dotenv.get("SEARCH_URL"))
                .orElse(System.getenv("SEARCH_URL"));

        return new AppConfig(
                urlIngestion,
                urlIndexing,
                urlSearch
        );
    }

}