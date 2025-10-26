package com.tahs.infrastructure;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.tahs.application.ports.IndexGateway;
import com.tahs.application.ports.SearchHit;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

public class HttpIndexGateway implements IndexGateway {
    private final HttpClient client = HttpClient.newHttpClient();

    @Override
    public List<SearchHit> search(String query) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/index/search?q=" + query))
                .GET().build();

        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        return new Gson().fromJson(res.body(), new TypeToken<List<SearchHit>>(){}.getType());
    }
}
