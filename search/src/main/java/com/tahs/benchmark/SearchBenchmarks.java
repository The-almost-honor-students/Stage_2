package com.tahs.benchmark;

import com.tahs.clients.IngestionClient;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@Fork(1)
@State(Scope.Benchmark)
public class SearchBenchmarks {

    @Param({"http://localhost:9090/search"})
    public String searchUrl;

    @Param({"book,science,death,war,love"})
    public String queryTerms;

    @Param({"10"})
    public int timeoutSec;

    @Param({"1,2,3"})
    public String bootstrapBookIds;

    @Param({"true"})
    public boolean enableBootstrapIfEmpty;

    private HttpClient client;
    private IngestionClient ingestionClient;
    private List<String> terms;
    private Iterator<String> rr;
    private List<String> bootstrapIds;

    @Setup(Level.Trial)
    public void setup() {
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeoutSec)).build();
        ingestionClient = new IngestionClient(client);
        terms = Arrays.stream(queryTerms.split(",")).map(String::trim).collect(Collectors.toList());
        rr = Iterators.cycle(terms);
        bootstrapIds = Arrays.stream(bootstrapBookIds.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    @Benchmark
    public void measureQueryResponseTime() throws Exception {
        String q = rr.next();
        ensureDataReady(q);
        String url = searchUrl + "?q=" + URLEncoder.encode(q, StandardCharsets.UTF_8);
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(timeoutSec)).GET().build();
        Instant start = Instant.now();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        Instant end = Instant.now();
        if (resp.statusCode() >= 400) throw new IllegalStateException("Bad response " + resp.statusCode());
        recordCpuAndMemory(Duration.between(start, end).toMillis());
    }

    private void ensureDataReady(String q) throws Exception {
        if (!enableBootstrapIfEmpty) return;
        if (hasResults(q)) return;
        for (String id : bootstrapIds) {
            try { ingestionClient.downloadBook(id); } catch (IOException | InterruptedException ignored) {}
        }
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (hasResults(q)) return;
            Thread.sleep(300);
        }
    }

    private boolean hasResults(String q) throws Exception {
        String url = searchUrl + "?q=" + URLEncoder.encode(q, StandardCharsets.UTF_8);
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(timeoutSec)).GET().build();
        HttpResponse<String> resp;
        try {
            resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (ConnectException e) {
            throw new IllegalStateException("Search service is not reachable at " + searchUrl, e);
        }
        if (resp.statusCode() >= 400) return false;
        String body = resp.body();
        if (body == null) return false;
        String t = body.trim();
        if (t.isEmpty() || "[]".equals(t) || "{}".equals(t)) return false;
        if (t.contains("\"results\":0") || t.contains("\"total\":0")) return false;
        return t.length() > 2;
    }

    private static final Path CSV = Paths.get("benchmarking_results/search_response.csv");

    private static synchronized void recordCpuAndMemory(double latencyMs) {
        try {
            OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            double cpuLoad = -1.0;
            if (os instanceof com.sun.management.OperatingSystemMXBean osm) cpuLoad = osm.getProcessCpuLoad() * 100.0;
            long usedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long totalMem = Runtime.getRuntime().maxMemory();
            if (!Files.exists(CSV)) {
                Files.createDirectories(CSV.getParent());
                try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(CSV))) {
                    w.println("timestamp_ms,latency_ms,cpu_percent,used_memory_mb,total_memory_mb");
                }
            }
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(CSV, StandardCharsets.UTF_8, StandardOpenOption.APPEND))) {
                w.printf(Locale.US, "%d,%.3f,%.2f,%.2f,%.2f%n",
                        System.currentTimeMillis(), latencyMs, cpuLoad, usedMem / 1e6, totalMem / 1e6);
            }
        } catch (IOException ignored) {}
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(SearchBenchmarks.class.getSimpleName())
                .forks(1)
                .warmupIterations(3)
                .measurementIterations(10)
                .timeUnit(TimeUnit.MILLISECONDS)
                .build();
        new Runner(opt).run();
        System.out.println("Results saved in: " + CSV.toAbsolutePath());
    }

    static class Iterators {
        static <T> Iterator<T> cycle(List<T> list) {
            return new Iterator<>() {
                int i = 0;
                public boolean hasNext() { return !list.isEmpty(); }
                public T next() { T t = list.get(i); i = (i + 1) % list.size(); return t; }
            };
        }
    }
}
