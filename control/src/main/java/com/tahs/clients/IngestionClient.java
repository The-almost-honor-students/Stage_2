package com.tahs.clients;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class IngestionClient {
    private final HttpClient httpClient;
    private final String urlIngestion;

    public IngestionClient(HttpClient httpClient, String urlIngestion) {
        this.httpClient = httpClient;
        this.urlIngestion = urlIngestion;
    }

    public HttpResponse<String> downloadBook(String bookId) throws IOException, InterruptedException {
        String urlIngestBook = this.urlIngestion + "/" + bookId;
        var request = HttpRequest.newBuilder(URI.create(urlIngestBook))
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        System.out.println("Download book " + bookId + "...");
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> status(String bookId) throws IOException, InterruptedException {
        String urlIndexingBook = this.urlIngestion + "/status/" + bookId;
        var request = HttpRequest.newBuilder(URI.create(urlIndexingBook))
                .header("Content-Type", "application/json")
                .GET().build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> list() throws IOException, InterruptedException {
        String urlIndexingBook = this.urlIngestion + "/list";
        var request = HttpRequest.newBuilder(URI.create(urlIndexingBook))
                .header("Content-Type", "application/json")
                .GET().build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

}
