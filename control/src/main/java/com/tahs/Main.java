package com.tahs;

import com.tahs.clients.IndexingClient;
import com.tahs.clients.IngestionClient;
import com.tahs.clients.SearchClient;
import com.tahs.config.AppConfig;
import com.tahs.orchestrator.Orchestrator;
import io.github.cdimascio.dotenv.Dotenv;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final int INTERVAL_SECONDS = 4;

    private static final ScheduledExecutorService SCHEDULER =
            Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args) {
        System.out.println("[CONTROL] Starting...");

        // 1) Cargar .env y variables de entorno
        var dotenv = Dotenv.configure().ignoreIfMissing().load();
        var appConfig = checkEnvVars(dotenv);

        // 2) Construir clientes y orquestador
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        Runnable controlPipelineStep = getRunnable(httpClient, appConfig);

        // 4) Programar la tarea
        SCHEDULER.scheduleAtFixedRate(
                controlPipelineStep,
                0,
                INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );

        // 5) Apagado ordenado al recibir SIGINT/SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[CONTROL] Shutdown signal received. Stopping scheduler...");
            SCHEDULER.shutdown();
            try {
                if (!SCHEDULER.awaitTermination(10, TimeUnit.SECONDS)) {
                    SCHEDULER.shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                SCHEDULER.shutdownNow();
            }
            System.out.println("[CONTROL] Scheduler stopped.");
        }));

        // 6) Mantener la app viva
        try {
            // Bloquea el hilo principal indefinidamente
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Runnable getRunnable(HttpClient httpClient, AppConfig appConfig) {
        IngestionClient ingestionClient = new IngestionClient(httpClient, appConfig.urlIngestion());
        IndexingClient indexingClient = new IndexingClient(httpClient, appConfig.urlIndex());
        SearchClient   searchClient   = new SearchClient(httpClient, appConfig.urlSearch());

        var orchestrator = new Orchestrator(ingestionClient, indexingClient, searchClient);

        // 3) Definir la tarea periódica (cada 4 segundos)
        // Evita que el scheduler muera por una excepción
        return () -> {
            try {
                System.out.println("[CONTROL] Executing pipeline");
                orchestrator.execute();
            } catch (Exception e) {
                // Evita que el scheduler muera por una excepción
                System.err.println("[CONTROL] Error executing pipeline: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        };
    }

    private static AppConfig checkEnvVars(Dotenv dotenv) {
        String urlIngestion = Optional.ofNullable(dotenv.get("INGESTION_URL"))
                .orElse(System.getenv("INGESTION_URL"));
        String urlIndexing = Optional.ofNullable(dotenv.get("INDEX_URL"))
                .orElse(System.getenv("INDEX_URL"));
        String urlSearch = Optional.ofNullable(dotenv.get("SEARCH_URL"))
                .orElse(System.getenv("SEARCH_URL"));

        // Validación básica de no-nulos / no-vacíos
        List<String> missing = new ArrayList<>();
        if (isBlank(urlIngestion)) missing.add("INGESTION_URL");
        if (isBlank(urlIndexing))  missing.add("INDEX_URL");
        if (isBlank(urlSearch))    missing.add("SEARCH_URL");

        if (!missing.isEmpty()) {
            throw new IllegalStateException("Missing required env vars: " + String.join(", ", missing));
        }

        return new AppConfig(urlIngestion, urlIndexing, urlSearch);
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}