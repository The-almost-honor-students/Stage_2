package com.tahs;

import com.tahs.clients.IndexingClient;
import com.tahs.clients.IngestionClient;
import com.tahs.clients.SearchClient;
import com.tahs.orchestrator.Orchestrator;
import java.net.http.HttpClient;
import java.util.concurrent.ThreadLocalRandom;


public class Main {

    public static void main(String[] args) {
        try {
            HttpClient httpClient = HttpClient.newHttpClient();
            IngestionClient ingestionClient = new IngestionClient(httpClient);
            IndexingClient indexingClient = new IndexingClient(httpClient);
            SearchClient searchClient = new SearchClient(httpClient);
            var orchestrator = new Orchestrator(ingestionClient, indexingClient, searchClient);
            var bookId = ThreadLocalRandom.current().nextInt(1, 70001);
            orchestrator.execute(String.valueOf(bookId));
        } catch (Exception e) {
            throw new RuntimeException("Error executing orchestrator", e);
        }

    }
}