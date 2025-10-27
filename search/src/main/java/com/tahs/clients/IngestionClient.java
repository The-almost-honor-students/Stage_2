package com.tahs.clients;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class IngestionClient {
    private static final String INGESTION_ENDPOINT = "http://localhost:7070/ingest";
    private final HttpClient httpClient;

    public IngestionClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public HttpResponse<String> downloadBook(String bookId) throws IOException, InterruptedException {
        String urlIngestBook = INGESTION_ENDPOINT + "/" + bookId;
        var request = HttpRequest.newBuilder(URI.create(urlIngestBook))
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        System.out.println("Download book " + bookId + "...");
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> status(String bookId) throws IOException, InterruptedException {
        String urlIndexingBook = INGESTION_ENDPOINT + "/status/" + bookId;
        var request = HttpRequest.newBuilder(URI.create(urlIndexingBook))
                .header("Content-Type", "application/json")
                .GET().build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> list() throws IOException, InterruptedException {
        String urlIndexingBook = INGESTION_ENDPOINT + "/list";
        var request = HttpRequest.newBuilder(URI.create(urlIndexingBook))
                .header("Content-Type", "application/json")
                .GET().build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

}
