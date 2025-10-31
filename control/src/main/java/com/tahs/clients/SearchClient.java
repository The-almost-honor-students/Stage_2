package com.tahs.clients;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

public class SearchClient {
    private final HttpClient httpClient;
    private final String urlSearch;

    public SearchClient(HttpClient httpClient, String urlSearch) {
        this.httpClient = httpClient;
        this.urlSearch = urlSearch;
    }

    public HttpResponse<String> search(String q) throws IOException, InterruptedException {
        String url = this.urlSearch + "/search?q=" + enc(q);
        var request = HttpRequest.newBuilder(URI.create(url))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> search(String q, String author, String language, Integer year) throws IOException, InterruptedException {
        Map<String, String> params = new LinkedHashMap<>();
        if (q != null && !q.isBlank()) params.put("q", q);
        if (author != null && !author.isBlank()) params.put("author", author);
        if (language != null && !language.isBlank()) params.put("language", language);
        if (year != null) params.put("year", String.valueOf(year));

        StringJoiner sj = new StringJoiner("&");
        for (var e : params.entrySet()) sj.add(enc(e.getKey()) + "=" + enc(e.getValue()));

        String url = this.urlSearch + "/search" + (params.isEmpty() ? "" : "?" + sj);
        var request = HttpRequest.newBuilder(URI.create(url))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> getMetadataById(String bookId) throws IOException, InterruptedException {
        String url = this.urlSearch + "/metadata/" + enc(bookId);
        var request = HttpRequest.newBuilder(URI.create(url))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> getBooksByTerm(String term) throws IOException, InterruptedException {
        String url = this.urlSearch + "/index/term?term=" + enc(term);
        var request = HttpRequest.newBuilder(URI.create(url))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String enc(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }
}
