package com.tahs.benchmark;

import com.tahs.clients.IndexingClient;
import com.tahs.clients.SearchClient;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
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

    @Param({"datalake"})
    public String datalakeRoot;

    @Param({"1-70000"})
    public String bootstrapRange;

    @Param({"false"})
    public boolean allowIndexingFromDatalake;

    @Param({"false"})
    public boolean rebuildIndex;

    @Param({"8"})
    public int bootstrapParallelism;

    @Param({"300"})
    public int bootstrapBatchSize;

    @Param({"300"})
    public int bootstrapTimeoutSec;

    @Param({"3"})
    public int httpRetries;

    @Param({"false"})
    public boolean failOnTimeout;

    private static final Path DATA_DIR = Paths.get("benchmarking_results/search/data");
    private static final Path PLOTS_DIR = Paths.get("benchmarking_results/search/plots");
    private static final Path SAMPLES_CSV = DATA_DIR.resolve("search_samples.csv");
    private static final Path SUMMARY_CSV = DATA_DIR.resolve("search_summary.csv");
    private static final Path THRPT_CSV = DATA_DIR.resolve("search_throughput_summary.csv");

    private HttpClient http;
    private List<String> terms;
    private Iterator<String> rrTerms;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(httpTimeoutSec)).build();
        IndexingClient indexingClient = new IndexingClient(http);
        SearchClient searchClient = new SearchClient(http);

        terms = Arrays.stream(queryTerms.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
        rrTerms = cycle(terms);

        Files.createDirectories(DATA_DIR);
        if (!Files.exists(SAMPLES_CSV)) {
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(SAMPLES_CSV))) {
                w.println("timestamp_ms,term,latency_ms,cpu_percent,used_memory_mb,total_memory_mb");
            }
        }

        if (allowIndexingFromDatalake) {
            bootstrapIfNeededFromDatalake();
        }
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
                try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(
                        SUMMARY_CSV, StandardCharsets.UTF_8,
                        StandardOpenOption.APPEND, StandardOpenOption.CREATE))) {
                    w.printf(Locale.US, "%.3f,%.3f,%.3f,%.2f,%.2f%n",
                            avgLat, p50Lat, p95Lat, avgCpu, avgMem);
                }
            }
        } catch (IOException ignored) {}
    }

    private void doSearchAndRecord(String term) throws Exception {
        String url = searchEndpoint + "?q=" + URLEncoder.encode(term, StandardCharsets.UTF_8);

        double latencyMs;
        boolean ok = false;

        Instant t0 = Instant.now();
        for (int attempt = 0; attempt < Math.max(1, httpRetries); attempt++) {
            try {
                var req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(url))
                        .timeout(Duration.ofSeconds(httpTimeoutSec))
                        .GET()
                        .build();

                var resp = http.send(req, java.net.http.HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (resp.statusCode() >= 400) {
                    continue;
                }
                ok = true;
                break;
            } catch (java.net.http.HttpTimeoutException | ConnectException e) {
                if (attempt + 1 < httpRetries) Thread.sleep(200L * (attempt + 1));
            }
        }
        Instant t1 = Instant.now();
        latencyMs = Duration.between(t0, t1).toNanos() / 1_000_000.0;

        if (!ok && failOnTimeout) {
            throw new java.net.http.HttpTimeoutException("request timed out after retries=" + httpRetries);
        }

        double cpuPct = processCpuPercent();
        double usedMb = usedMemoryMb();
        double totalMb = totalMemoryMb();

        synchronized (SearchBenchmarks.class) {
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(
                    SAMPLES_CSV, StandardCharsets.UTF_8, StandardOpenOption.APPEND))) {
                w.printf(Locale.US, "%d,%s,%.3f,%.2f,%.2f,%.2f%n",
                        System.currentTimeMillis(), term, latencyMs, cpuPct, usedMb, totalMb);
            }
        }
    }

    private void bootstrapIfNeededFromDatalake() throws Exception {
        if (hasAnyResults(terms.isEmpty() ? "book" : terms.get(0), Math.max(5, httpTimeoutSec))) return;
        Set<Integer> idsInLake = findBookIdsInDatalake(Paths.get(datalakeRoot));
        if (idsInLake.isEmpty()) return;
    }

    private static Set<Integer> findBookIdsInDatalake(Path root) {
        if (!Files.isDirectory(root)) return Set.of();
        Set<Integer> ids = new HashSet<>();
        try (Stream<Path> days = Files.list(root)) {
            days.filter(Files::isDirectory).forEach(day -> {
                try (Stream<Path> hours = Files.list(day)) {
                    hours.filter(Files::isDirectory).forEach(hour -> {
                        try (Stream<Path> files = Files.list(hour)) {
                            files.filter(Files::isRegularFile).forEach(p -> {
                                String name = p.getFileName().toString();
                                int dot = name.indexOf('.');
                                if (dot > 0) {
                                    try {
                                        int id = Integer.parseInt(name.substring(0, dot));
                                        ids.add(id);
                                    } catch (NumberFormatException ignored) {}
                                }
                            });
                        } catch (IOException ignored) {}
                    });
                } catch (IOException ignored) {}
            });
        } catch (IOException ignored) {}
        return ids;
    }

    private boolean hasAnyResults(String term, int timeoutSec) {
        try {
            String url = searchEndpoint + "?q=" + URLEncoder.encode(term, StandardCharsets.UTF_8);
            var req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(url))
                    .timeout(Duration.ofSeconds(Math.min(timeoutSec, 3)))
                    .GET().build();

            var resp = http.send(req, java.net.http.HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() >= 400) return false;
            String t = Optional.ofNullable(resp.body()).orElse("").trim();
            return !(t.isEmpty() || "[]".equals(t) || "{}".equals(t) || t.contains("\"total\":0"));
        } catch (Exception e) {
            return false;
        }
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
        Files.createDirectories(DATA_DIR);
        Files.createDirectories(PLOTS_DIR);

        Options opt1 = new OptionsBuilder()
                .include(SearchBenchmarks.class.getSimpleName())
                .forks(1)
                .warmupIterations(5)
                .measurementIterations(10)
                .timeUnit(TimeUnit.MILLISECONDS)
                .result(SUMMARY_CSV.toString())
                .resultFormat(ResultFormatType.CSV)
                .build();
        new Runner(opt1).run();

        int[] threads = {1, 2, 4, 8};
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(THRPT_CSV))) {
            w.println("threads,ops_per_sec");
            for (int t : threads) {
                Options opt2 = new OptionsBuilder()
                        .include(SearchBenchmarks.class.getSimpleName())
                        .mode(Mode.Throughput)
                        .threads(t)
                        .timeUnit(TimeUnit.SECONDS)
                        .forks(1)
                        .resultFormat(ResultFormatType.TEXT)
                        .build();
                Collection<RunResult> results = new Runner(opt2).run();
                for (RunResult r : results) {
                    double thr = r.getPrimaryResult().getScore();
                    w.printf(Locale.US, "%d,%.3f%n", t, thr);
                }
            }
        }

        Map<Integer, Double> thr = new TreeMap<>();
        try (BufferedReader br = Files.newBufferedReader(THRPT_CSV)) {
            String line; boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; }
                String[] p = line.split(",");
                thr.put(Integer.parseInt(p[0]), Double.parseDouble(p[1]));
            }
        }
        plotThroughput(thr, PLOTS_DIR.resolve("search_throughput_vs_threads.png"));
        System.out.println("CSV and plots created in " + PLOTS_DIR.toAbsolutePath());
    }

    private static void plotThroughput(Map<Integer, Double> data, Path outPng) throws IOException {
        int W = 900, H = 600, L = 80, B = 60;
        int PW = W - 2 * L, PH = H - B - 100;
        BufferedImage img = new BufferedImage(W, H, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, W, H);
        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.BOLD, 18));
        g.drawString("Search — Throughput vs Threads", L, 40);

        List<Integer> xs = new ArrayList<>(data.keySet());
        double maxY = data.values().stream().mapToDouble(Double::doubleValue).max().orElse(1);
        g.drawLine(L, H - B, L + PW, H - B);
        g.drawLine(L, H - B, L, H - B - PH);
        g.setColor(new Color(30, 144, 255));
        int prevX = -1, prevY = -1;
        for (int i = 0; i < xs.size(); i++) {
            int x = L + (int)((i / (double)(xs.size()-1)) * PW);
            int y = (int)(H - B - (data.get(xs.get(i)) / maxY) * PH);
            g.fillOval(x-4, y-4, 8, 8);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
            g.drawString(String.valueOf(xs.get(i)), x-5, H - B + 20);
        }
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }
}
