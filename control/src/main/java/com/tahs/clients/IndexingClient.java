package com.tahs.clients;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class IndexingClient {
    private static final String INDEXING_ENDPOINT = "http://localhost:8080/index";
    private final HttpClient httpClient;

    public IndexingClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public HttpResponse<String> updateIndexForBook(String bookId) throws IOException, InterruptedException {
        String urlIndexingBook = INDEXING_ENDPOINT + "/update/" + bookId;
        var request = HttpRequest.newBuilder(URI.create(urlIndexingBook))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        System.out.println("Index book " + bookId + "...");
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> rebuildIndexForBook() throws IOException, InterruptedException {
        String urlIndexingBook = INDEXING_ENDPOINT + "/rebuild";
        var request = HttpRequest.newBuilder(URI.create(urlIndexingBook))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> status() throws IOException, InterruptedException {
        String urlIndexingBook = INDEXING_ENDPOINT + "/status";
        var request = HttpRequest.newBuilder(URI.create(urlIndexingBook))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

}
