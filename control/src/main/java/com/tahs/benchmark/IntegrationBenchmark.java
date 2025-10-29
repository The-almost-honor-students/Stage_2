package com.tahs.benchmark;

import com.tahs.clients.IndexingClient;
import com.tahs.clients.IngestionClient;
import com.tahs.orchestrator.Orchestrator;
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
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 8)
@Fork(1)
@State(Scope.Benchmark)
public class IntegrationBenchmark {
    //TODO: implement better plots with measures
    @Param({"http://localhost:7070"})
    public String ingestionBaseUrl;

    @Param({"http://localhost:8080"})
    public String indexingBaseUrl;

    @Param({"1-70000"})
    public String bookIdRange;

    @Param({"10"})
    public int httpTimeoutSec;

    @Param({"3"})
    public int httpRetries;

    @Param({"false"})
    public boolean failOnTimeout;

    private static final String DEVICE =
            System.getProperty("bench.device", "MacBook Air M3 (2024)");

    private static final String DEVICE_SLUG =
            DEVICE.trim().toLowerCase().replaceAll("[^a-z0-9]+", "-").replaceAll("^-|-$", "");

    private static final Path DATA_DIR  = Paths.get("benchmarking_results", "integration", "data");
    private static final Path PLOTS_DIR = Paths.get("benchmarking_results", "integration", "plots");

    private static final Path SAMPLES_CSV = DATA_DIR.resolve("integration_samples_" + DEVICE_SLUG + ".csv");
    private static final Path SUMMARY_CSV = DATA_DIR.resolve("integration_summary_" + DEVICE_SLUG + ".csv");
    private static final Path THRPT_CSV   = DATA_DIR.resolve("integration_throughput_summary_" + DEVICE_SLUG + ".csv");

    private HttpClient http;
    private Orchestrator orchestrator;
    private List<String> bookIds;
    private Random rnd;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        Files.createDirectories(DATA_DIR);
        Files.createDirectories(PLOTS_DIR);
        if (!Files.exists(SAMPLES_CSV)) {
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(SAMPLES_CSV))) {
                w.println("timestamp_ms,book_id,latency_ms,cpu_percent,used_memory_mb,total_memory_mb");
            }
        }
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(httpTimeoutSec)).build();
        IngestionClient ingestionClient = new IngestionClient(http);
        IndexingClient  indexingClient  = new IndexingClient(http);
        orchestrator = new Orchestrator(ingestionClient, indexingClient);
        bookIds = parseRange(bookIdRange);
        rnd = new Random(42);
    }

    private static List<String> parseRange(String spec) {
        spec = spec.trim();
        if (spec.contains("-")) {
            String[] p = spec.split("-");
            int lo = Integer.parseInt(p[0].trim());
            int hi = Integer.parseInt(p[1].trim());
            if (hi < lo) { int t = lo; lo = hi; hi = t; }
            List<String> out = new ArrayList<>(hi - lo + 1);
            for (int i = lo; i <= hi; i++) out.add(Integer.toString(i));
            return out;
        }
        String[] p = spec.split(",");
        List<String> out = new ArrayList<>();
        for (String s : p) {
            String t = s.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    @Benchmark
    public void orchestrate_once() throws Exception {
        String bookId = bookIds.get(rnd.nextInt(bookIds.size()));
        doExecuteAndRecord(bookId);
    }

    private void doExecuteAndRecord(String bookId) throws Exception {
        double latencyMs;
        boolean ok = false;
        Instant t0 = Instant.now();
        for (int attempt = 0; attempt < Math.max(1, httpRetries); attempt++) {
            try {
                orchestrator.execute(bookId);
                ok = true;
                break;
            } catch (ConnectException | java.net.http.HttpTimeoutException e) {
                if (attempt + 1 < httpRetries) Thread.sleep(200L * (attempt + 1));
            }
        }
        Instant t1 = Instant.now();
        latencyMs = Duration.between(t0, t1).toNanos() / 1_000_000.0;
        if (!ok && failOnTimeout) throw new java.net.http.HttpTimeoutException("orchestrator timed out after retries=" + httpRetries);
        double cpuPct  = processCpuPercent();
        double usedMb  = usedMemoryMb();
        double totalMb = totalMemoryMb();
        synchronized (IntegrationBenchmark.class) {
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(SAMPLES_CSV, StandardCharsets.UTF_8, StandardOpenOption.APPEND))) {
                w.printf(Locale.US, "%d,%s,%.3f,%.2f,%.2f,%.2f%n",
                        System.currentTimeMillis(), bookId, latencyMs, cpuPct, usedMb, totalMb);
            }
        }
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
                try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(SUMMARY_CSV, StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE))) {
                    w.printf(Locale.US, "%.3f,%.3f,%.3f,%.2f,%.2f%n", avgLat, p50Lat, p95Lat, avgCpu, avgMem);
                }
            }
        } catch (IOException ignored) {}
    }

    public static void main(String[] args) throws Exception {
        Files.createDirectories(DATA_DIR);
        Files.createDirectories(PLOTS_DIR);
        Options opt1 = new OptionsBuilder()
                .include(IntegrationBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(5)
                .measurementIterations(8)
                .timeUnit(TimeUnit.MILLISECONDS)
                .result(SUMMARY_CSV.toString())
                .resultFormat(ResultFormatType.CSV)
                .build();
        new Runner(opt1).run();

        int[] threads = {1, 2, 4, 8, 16};
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(THRPT_CSV))) {
            w.println("threads,ops_per_sec");
            for (int t : threads) {
                Options opt2 = new OptionsBuilder()
                        .include(IntegrationBenchmark.class.getSimpleName())
                        .mode(org.openjdk.jmh.annotations.Mode.Throughput)
                        .threads(t)
                        .warmupIterations(5)
                        .measurementIterations(8)
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

        Map<Integer, Double> thr = readThrpt(THRPT_CSV);
        List<Double> latencies = readLatencies(SAMPLES_CSV);
        List<long[]> tsCpuMem = readCpuMemSeries(SAMPLES_CSV);

        plotThroughput(thr, PLOTS_DIR.resolve("integration_throughput_vs_threads_" + DEVICE_SLUG + ".png"));
        plotLatencyHistogram(latencies, 30, PLOTS_DIR.resolve("integration_latency_hist_" + DEVICE_SLUG + ".png"));
        plotCpuSeries(tsCpuMem, PLOTS_DIR.resolve("integration_cpu_over_time_" + DEVICE_SLUG + ".png"));
        plotMemSeries(tsCpuMem, PLOTS_DIR.resolve("integration_memory_over_time_" + DEVICE_SLUG + ".png"));

        System.out.println("CSV and plots in: " + PLOTS_DIR.toAbsolutePath());
    }

    private static Map<Integer, Double> readThrpt(Path csv) throws IOException {
        Map<Integer, Double> m = new TreeMap<>();
        try (BufferedReader br = Files.newBufferedReader(csv)) {
            String line; boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; }
                String[] p = line.split(",");
                m.put(Integer.parseInt(p[0].trim()), Double.parseDouble(p[1].trim()));
            }
        }
        return m;
    }

    private static List<Double> readLatencies(Path csv) throws IOException {
        List<Double> lat = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(csv)) {
            String line; boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; }
                String[] p = line.split(",", -1);
                if (p.length >= 3) lat.add(parseDouble(p[2]));
            }
        }
        return lat;
    }

    private static List<long[]> readCpuMemSeries(Path csv) throws IOException {
        List<long[]> rows = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(csv)) {
            String line; boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; }
                String[] p = line.split(",", -1);
                if (p.length >= 6) {
                    long ts = Long.parseLong(p[0].trim());
                    long cpu = Math.round(parseDouble(p[3]));
                    long used = Math.round(parseDouble(p[4]));
                    long total = Math.round(parseDouble(p[5]));
                    rows.add(new long[]{ts, cpu, used, total});
                }
            }
        }
        return rows;
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
        Collections.sort(sorted);
        double rank = (p / 100.0) * (sorted.size() - 1);
        int lo = (int) Math.floor(rank);
        int hi = (int) Math.ceil(rank);
        if (lo == hi) return sorted.get(lo);
        double w = rank - lo;
        return sorted.get(lo) * (1 - w) + sorted.get(hi) * w;
    }

    private static void plotThroughput(Map<Integer, Double> data, Path outPng) throws IOException {
        int W = 900, H = 600, L = 80, B = 80;
        int PW = W - 2 * L, PH = H - B - 100;
        BufferedImage img = new BufferedImage(W, H, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, W, H);
        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.BOLD, 18));
        g.drawString("Throughput vs Threads — " + DEVICE, L, 40);
        drawAxes(g, L, B, W, H, PH, "Threads", "Throughput (ops/sec)");
        g.setColor(new Color(30, 144, 255));
        List<Integer> xs = new ArrayList<>(data.keySet());
        double maxY = data.values().stream().mapToDouble(Double::doubleValue).max().orElse(1);
        int prevX = -1, prevY = -1;
        for (int i = 0; i < xs.size(); i++) {
            int x = L + (int) ((i / (double) Math.max(1, xs.size() - 1)) * PW);
            int y = (int) (H - B - (data.get(xs.get(i)) / maxY) * PH);
            g.fillOval(x - 4, y - 4, 8, 8);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    private static void plotLatencyHistogram(List<Double> lat, int bins, Path outPng) throws IOException {
        if (lat.isEmpty()) return;
        double max = lat.stream().mapToDouble(Double::doubleValue).max().orElse(1);
        double min = lat.stream().mapToDouble(Double::doubleValue).min().orElse(0);
        if (max <= min) max = min + 1;
        int[] hist = new int[bins];
        for (double v : lat) {
            int b = (int) ((v - min) / (max - min) * bins);
            if (b >= bins) b = bins - 1;
            hist[b]++;
        }
        int maxCount = Arrays.stream(hist).max().orElse(1);
        int W = 900, H = 600, L = 80, B = 80, PW = W - 2 * L, PH = H - B - 100;
        BufferedImage img = new BufferedImage(W, H, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, W, H);
        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.BOLD, 18));
        g.drawString("Latency Histogram (ms) — " + DEVICE, L, 40);
        drawAxes(g, L, B, W, H, PH, "Latency (ms)", "Frequency");
        g.setColor(new Color(34, 139, 34));
        int barW = Math.max(1, PW / bins);
        for (int i = 0; i < bins; i++) {
            int h = (int) ((hist[i] / (double) maxCount) * PH);
            int x = L + i * barW;
            g.fillRect(x, H - B - h, barW - 2, h);
        }
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    private static void plotCpuSeries(List<long[]> series, Path outPng) throws IOException {
        if (series.isEmpty()) return;
        int W = 900, H = 600, L = 80, B = 80, PW = W - 2 * L, PH = H - B - 100;
        BufferedImage img = new BufferedImage(W, H, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, W, H);
        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.BOLD, 18));
        g.drawString("CPU (%) over Time — " + DEVICE, L, 40);
        drawAxes(g, L, B, W, H, PH, "Time (ms)", "CPU (%)");
        long t0 = series.get(0)[0], tN = series.get(series.size() - 1)[0], span = Math.max(1, tN - t0);
        g.setColor(new Color(75, 0, 130));
        int prevX = -1, prevY = -1;
        for (long[] r : series) {
            long ts = r[0], cpu = r[1];
            int x = L + (int) ((ts - t0) * 1.0 * PW / span);
            int y = (int) (H - B - (cpu / 100.0) * PH);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    private static void plotMemSeries(List<long[]> series, Path outPng) throws IOException {
        if (series.isEmpty()) return;
        int W = 900, H = 600, L = 80, B = 80, PW = W - 2 * L, PH = H - B - 100;
        BufferedImage img = new BufferedImage(W, H, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, W, H);
        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.BOLD, 18));
        g.drawString("Memory (MB) over Time — " + DEVICE, L, 40);
        drawAxes(g, L, B, W, H, PH, "Time (ms)", "Used Memory (MB)");
        long t0 = series.get(0)[0], tN = series.get(series.size() - 1)[0], span = Math.max(1, tN - t0);
        long maxUsed = series.stream().mapToLong(r -> r[2]).max().orElse(1);
        g.setColor(new Color(220, 20, 60));
        int prevX = -1, prevY = -1;
        for (long[] r : series) {
            long ts = r[0], used = r[2];
            int x = L + (int) ((ts - t0) * 1.0 * PW / span);
            int y = (int) (H - B - (used / (double) maxUsed) * PH);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    private static void drawAxes(Graphics2D g, int L, int B, int W, int H, int PH, String xLabel, String yLabel) {
        g.setColor(Color.BLACK);
        g.drawLine(L, H - B, W - L, H - B);
        g.drawLine(L, H - B, L, H - B - PH);
        g.setFont(new Font("SansSerif", Font.PLAIN, 14));
        g.drawString(xLabel, W / 2 - (xLabel.length() * 3), H - 30);
        g.rotate(-Math.PI / 2);
        g.drawString(yLabel, -H / 2 - (yLabel.length() * 3), 30);
        g.rotate(Math.PI / 2);
    }
}
