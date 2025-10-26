package com.tahs.clients;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

public class SearchClient {
    private static final String SEARCH_ENDPOINT = "http://localhost:8080/search";
    private final HttpClient httpClient;

    public SearchClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public String search(String query) throws IOException, InterruptedException {
        String encoded = URLEncoder.encode(query, StandardCharsets.UTF_8);
        String url = SEARCH_ENDPOINT + "/search?q=" + encoded;
        var request = HttpRequest.newBuilder(URI.create(url))
                .GET().build();
        HttpResponse<String> response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
}
