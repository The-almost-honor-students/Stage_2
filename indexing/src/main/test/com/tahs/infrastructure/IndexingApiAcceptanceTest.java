package com.tahs;

import io.javalin.Javalin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IndexingApiAcceptanceTest {
    private static Javalin app;
    @BeforeAll
    static void setup() {
        app = Main.createApp();
    }
    @AfterAll
    static void stop() { app.stop(); }

    @Test
    void update_index_for_book() throws IOException, InterruptedException {
        var indexUpdateCall = CreateHttpPostCall("http://localhost:8080/index/update/1");

        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(indexUpdateCall, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertEquals("{\"book_id\":\"1\",\"index\":\"updated\"}", response.body());
    }

    private HttpRequest CreateHttpPostCall(String url) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
    }
}
