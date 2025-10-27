package com.tahs.benchmark;

import com.tahs.clients.IngestionClient;
import com.tahs.clients.IndexingClient;
import com.tahs.clients.SearchClient;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.ConnectException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@Fork(1)
@State(Scope.Benchmark)
public class SearchBenchmarks {

    @Param({"http://localhost:9090/search"})
    public String searchEndpoint;

    @Param({"book,science,death,war,love"})
    public String queryTerms;

    @Param({"10"})
    public int httpTimeoutSec;

    @Param({"1-1000"})
    public String bootstrapRange;

    @Param({"true"})
    public boolean enableBootstrap;

    @Param({"true"})
    public boolean rebuildIndexAfterIngestion;

    @Param({"200"})
    public int bootstrapBatchSize;

    @Param({"8"})
    public int bootstrapParallelism;

    @Param({"300"})
    public int bootstrapTimeoutSec;

    private static final Path SAMPLES_CSV = Paths.get("benchmarking_results/search/search_samples.csv");
    private static final Path SUMMARY_CSV = Paths.get("benchmarking_results/search/search_summary.csv");

    private HttpClient http;
    private IngestionClient ingestionClient;
    private IndexingClient indexingClient;
    private SearchClient searchClient;
    private List<String> terms;
    private Iterator<String> rrTerms;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(httpTimeoutSec)).build();
        ingestionClient = new IngestionClient(http);
        indexingClient  = new IndexingClient(http);
        searchClient    = new SearchClient(http);

        terms    = Arrays.stream(queryTerms.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
        rrTerms  = cycle(terms);

        Files.createDirectories(SAMPLES_CSV.getParent());
        if (!Files.exists(SAMPLES_CSV)) {
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(SAMPLES_CSV))) {
                w.println("timestamp_ms,term,latency_ms,cpu_percent,used_memory_mb,total_memory_mb");
            }
        }

        if (enableBootstrap) bootstrapIfNeeded();
    }

    @Benchmark
    public void searchUnderLoad() throws Exception {
        String term = rrTerms.next();
        doSearchAndRecord(term);
    }

    @TearDown(Level.Trial)
    public void summarize() {
        try {
            if (!Files.exists(SUMMARY_CSV)) {
                Files.createDirectories(SUMMARY_CSV.getParent());
                try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(SUMMARY_CSV))) {
                    w.println("avg_latency_ms,p50_latency_ms,p95_latency_ms,avg_cpu_percent,avg_used_memory_mb");
                }
            }
            List<Double> lat = new ArrayList<>();
            List<Double> cpu = new ArrayList<>();
            List<Double> mem = new ArrayList<>();

            try (BufferedReader br = Files.newBufferedReader(SAMPLES_CSV)) {
                String line; boolean first = true;
                while ((line = br.readLine()) != null) {
                    if (first) { first = false; continue; }
                    if (line.isBlank()) continue;
                    String[] p = line.split(",", -1);
                    if (p.length < 6) continue;
                    lat.add(parseDouble(p[2]));
                    cpu.add(parseDouble(p[3]));
                    mem.add(parseDouble(p[4]));
                }
            }

            if (!lat.isEmpty()) {
                Collections.sort(lat);
                double avgLat = lat.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
                double p50Lat = percentile(lat, 50);
                double p95Lat = percentile(lat, 95);
                double avgCpu = cpu.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
                double avgMem = mem.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
                try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(SUMMARY_CSV, StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE))) {
                    w.printf(Locale.US, "%.3f,%.3f,%.3f,%.2f,%.2f%n", avgLat, p50Lat, p95Lat, avgCpu, avgMem);
                }
            }
        } catch (IOException ignored) {}
    }

    private void doSearchAndRecord(String term) throws Exception {
        String url = searchEndpoint + "?q=" + URLEncoder.encode(term, StandardCharsets.UTF_8);
        var req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(url))
                .timeout(Duration.ofSeconds(httpTimeoutSec)).GET().build();

        Instant t0 = Instant.now();
        var resp = http.send(req, java.net.http.HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        Instant t1 = Instant.now();

        if (resp.statusCode() >= 400) throw new IllegalStateException("HTTP " + resp.statusCode());

        double latencyMs = Duration.between(t0, t1).toNanos() / 1_000_000.0;
        double cpuPct    = processCpuPercent();
        double usedMb    = usedMemoryMb();
        double totalMb   = totalMemoryMb();

        synchronized (SearchBenchmarks.class) {
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(SAMPLES_CSV, StandardCharsets.UTF_8, StandardOpenOption.APPEND))) {
                w.printf(Locale.US, "%d,%s,%.3f,%.2f,%.2f,%.2f%n",
                        System.currentTimeMillis(), term, latencyMs, cpuPct, usedMb, totalMb);
            }
        }
    }

    private void bootstrapIfNeeded() throws Exception {
        if (hasAnyResults(terms.isEmpty() ? "book" : terms.get(0), bootstrapTimeoutSec)) return;

        int[] r = parseRange(bootstrapRange);
        List<Integer> allIds = IntStream.rangeClosed(r[0], r[1]).boxed().collect(Collectors.toList());

        ExecutorService pool = Executors.newFixedThreadPool(bootstrapParallelism);
        try {
            for (int i = 0; i < allIds.size(); i += bootstrapBatchSize) {
                List<Integer> batch = allIds.subList(i, Math.min(i + bootstrapBatchSize, allIds.size()));
                List<Callable<Void>> tasks = new ArrayList<>(batch.size());
                for (Integer id : batch) {
                    tasks.add(() -> {
                        try { ingestionClient.downloadBook(String.valueOf(id)); } catch (Exception ignored) {}
                        return null;
                    });
                }
                pool.invokeAll(tasks);
            }
        } finally {
            pool.shutdown();
            pool.awaitTermination(bootstrapTimeoutSec, TimeUnit.SECONDS);
        }

        if (rebuildIndexAfterIngestion) {
            try { indexingClient.rebuildIndexForBook(); } catch (Exception ignored) {}
        } else {
            ExecutorService indexPool = Executors.newFixedThreadPool(bootstrapParallelism);
            try {
                List<Callable<Void>> tasks = new ArrayList<>(allIds.size());
                for (Integer id : allIds) {
                    tasks.add(() -> {
                        try { indexingClient.updateIndexForBook(String.valueOf(id)); } catch (Exception ignored) {}
                        return null;
                    });
                }
                for (int i = 0; i < tasks.size(); i += bootstrapBatchSize) {
                    List<Callable<Void>> slice = tasks.subList(i, Math.min(i + bootstrapBatchSize, tasks.size()));
                    indexPool.invokeAll(slice);
                }
            } finally {
                poolShutdownQuiet(indexPool);
            }
        }

        long deadline = System.currentTimeMillis() + bootstrapTimeoutSec * 1000L;
        while (System.currentTimeMillis() < deadline) {
            if (hasAnyResults(terms.isEmpty() ? "book" : terms.get(0), Math.max(5, httpTimeoutSec))) return;
            Thread.sleep(500);
        }
        throw new IllegalStateException("Bootstrap timed out waiting for search to return results");
    }

    private boolean hasAnyResults(String term, int timeoutSec) throws Exception {
        String url = searchEndpoint + "?q=" + URLEncoder.encode(term, StandardCharsets.UTF_8);
        var req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSec)).GET().build();

        int retries = 3;
        for (int i = 0; i < retries; i++) {
            try {
                var resp = http.send(req, java.net.http.HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (resp.statusCode() >= 400) return false;
                String t = Optional.ofNullable(resp.body()).orElse("").trim();
                if (t.isEmpty() || "[]".equals(t) || "{}".equals(t)) return false;
                if (t.contains("\"results\":0") || t.contains("\"total\":0")) return false;
                return t.length() > 2;
            } catch (java.net.http.HttpTimeoutException e) {
                if (i == retries - 1) throw e;
                Thread.sleep(300L * (i + 1));
            } catch (ConnectException e) {
                if (i == retries - 1) throw new IllegalStateException("Search not reachable at " + searchEndpoint, e);
                Thread.sleep(300L * (i + 1));
            }
        }
        return false;
    }

    private static void poolShutdownQuiet(ExecutorService es) {
        es.shutdown();
        try { es.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
    }

    private static double processCpuPercent() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean osm) {
            double v = osm.getProcessCpuLoad();
            if (v >= 0) return v * 100.0;
        }
        return -1.0;
    }

    private static double usedMemoryMb() {
        long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        return used / 1e6;
    }

    private static double totalMemoryMb() {
        long total = Runtime.getRuntime().maxMemory();
        return total / 1e6;
    }

    private static int[] parseRange(String spec) {
        String s = spec.trim();
        String[] p = s.split("-");
        if (p.length != 2) throw new IllegalArgumentException("bootstrapRange must be a-b");
        int a = Integer.parseInt(p[0].trim());
        int b = Integer.parseInt(p[1].trim());
        if (a > b) { int t = a; a = b; b = t; }
        return new int[]{a, b};
    }

    private static double parseDouble(String s) {
        try { return Double.parseDouble(s.trim()); } catch (Exception e) { return Double.NaN; }
    }

    private static double percentile(List<Double> sorted, double p) {
        if (sorted.isEmpty()) return Double.NaN;
        double rank = (p / 100.0) * (sorted.size() - 1);
        int lo = (int) Math.floor(rank);
        int hi = (int) Math.ceil(rank);
        if (lo == hi) return sorted.get(lo);
        double w = rank - lo;
        return sorted.get(lo) * (1 - w) + sorted.get(hi) * w;
    }

    private static <T> Iterator<T> cycle(List<T> list) {
        return new Iterator<>() {
            int i = 0;
            public boolean hasNext() { return !list.isEmpty(); }
            public T next() { T t = list.get(i); i = (i + 1) % list.size(); return t; }
        };
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
        System.out.println("Samples:  " + SAMPLES_CSV.toAbsolutePath());
        System.out.println("Summary:  " + SUMMARY_CSV.toAbsolutePath());
    }
}
