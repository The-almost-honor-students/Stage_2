package com.tahs.clients;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class IndexingClient {
    private final HttpClient httpClient;
    private final String urlIndexing;

    public IndexingClient(HttpClient httpClient, String urlIndexing) {
        this.httpClient = httpClient;
        this.urlIndexing = urlIndexing;
    }

    public HttpResponse<String> updateIndexForBook(String bookId) throws IOException, InterruptedException {
        String urlIndexingBook = this.urlIndexing + "/index/update/" + bookId;
        var request = HttpRequest.newBuilder(URI.create(urlIndexingBook))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        System.out.println("Index book " + bookId + "...");
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> rebuildIndexForBook() throws IOException, InterruptedException {
        String urlIndexingBook = this.urlIndexing + "/index/rebuild";
        var request = HttpRequest.newBuilder(URI.create(urlIndexingBook))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> status() throws IOException, InterruptedException {
        String urlIndexingBook = this.urlIndexing + "/index/status";
        var request = HttpRequest.newBuilder(URI.create(urlIndexingBook))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

}
