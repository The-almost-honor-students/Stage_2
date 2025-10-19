package com.tahs.infrastructure;

import com.tahs.Main;
import io.javalin.Javalin;
import org.junit.jupiter.api.*;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;

import static com.tahs.Main.createApp;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IndexingApiAcceptanceTest {
    private static Javalin app;
    @BeforeAll
    static void setup() {
        app = createApp().start(8080);
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
