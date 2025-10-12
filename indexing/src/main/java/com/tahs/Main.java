package com.tahs;

import io.javalin.Javalin;
import com.google.gson.Gson;

import java.util.Map;

public class Main {
    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(8080);
        Gson gson = new Gson();

        app.get("/status", ctx -> {
            Map<String, String> status = Map.of(
                    "service", "example-service",
                    "status", "running"
            );
            ctx.result(gson.toJson(status));
        });

        app.get("/data", ctx -> {
            Map<String, String> data = Map.of(
                    "service", "example-service",
                    "data", "running"
            );
            ctx.result(gson.toJson(data));
        });

    }
}
