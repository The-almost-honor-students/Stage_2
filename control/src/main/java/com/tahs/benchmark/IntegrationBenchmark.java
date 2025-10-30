package com.tahs.benchmark;

import com.tahs.clients.IndexingClient;
import com.tahs.clients.IngestionClient;
import com.tahs.clients.SearchClient;
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
import java.text.DecimalFormat;
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
    //TODO implement search client
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

    @Param({"300"})
    public int maxBooksPerIteration;

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

    private List<String> iterBookIds;
    private int iterCursor;

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
        SearchClient    searchClient    = new SearchClient(http);
        orchestrator = new Orchestrator(ingestionClient, indexingClient, searchClient);

        bookIds = parseRange(bookIdRange);
        rnd = new Random(42);

        Collections.shuffle(bookIds, rnd);
        int n = Math.min(maxBooksPerIteration, bookIds.size());
        iterBookIds = new ArrayList<>(bookIds.subList(0, n)); // ← siempre los mismos 500 en todo el trial
        iterCursor = 0;
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        List<String> shuffled = new ArrayList<>(bookIds);
        Collections.shuffle(shuffled, rnd);
        int n = Math.min(maxBooksPerIteration, shuffled.size());
        iterBookIds = new ArrayList<>(shuffled.subList(0, n));
        iterCursor = 0;
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
        String bookId = iterBookIds.get((iterCursor++) % iterBookIds.size());
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

    @Benchmark
    public void search_once() throws Exception {
        String query = iterBookIds.get((iterCursor++) % iterBookIds.size());
        doSearchAndRecord(query);
    }

    private void doSearchAndRecord(String query) throws Exception {
        boolean ok = false;
        Instant t0 = Instant.now();
        for (int attempt = 0; attempt < Math.max(1, httpRetries); attempt++) {
            try {
                orchestrator.getSearchClient().search(query);
                ok = true;
                break;
            } catch (ConnectException | java.net.http.HttpTimeoutException e) {
                if (attempt + 1 < httpRetries) Thread.sleep(200L * (attempt + 1));
            }
        }
        Instant t1 = Instant.now();
        double latencyMs = Duration.between(t0, t1).toNanos() / 1_000_000.0;
        if (!ok && failOnTimeout) {
            throw new java.net.http.HttpTimeoutException("search timed out after retries=" + httpRetries);
        }
        double cpuPct  = processCpuPercent();
        double usedMb  = usedMemoryMb();
        double totalMb = totalMemoryMb();
        synchronized (IntegrationBenchmark.class) {
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(
                    SAMPLES_CSV, StandardCharsets.UTF_8, StandardOpenOption.APPEND))) {
                w.printf(Locale.US, "%d,%s,%.3f,%.2f,%.2f,%.2f%n",
                        System.currentTimeMillis(), query, latencyMs, cpuPct, usedMb, totalMb);
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

    private static class NiceTicks {
        final double min, max, step;
        NiceTicks(double min, double max, double step) { this.min=min; this.max=max; this.step=step; }
    }
    private static NiceTicks niceTicks(double dataMin, double dataMax, int maxTicks) {
        if (Double.isNaN(dataMin) || Double.isNaN(dataMax)) { dataMin = 0; dataMax = 1; }
        if (dataMax == dataMin) dataMax = dataMin + 1;
        double range = niceNum(dataMax - dataMin, false);
        double step = niceNum(range / Math.max(2, (maxTicks - 1)), true);
        double niceMin = Math.floor(dataMin / step) * step;
        double niceMax = Math.ceil(dataMax / step) * step;
        return new NiceTicks(niceMin, niceMax, step);
    }
    private static double niceNum(double x, boolean round) {
        double exp = Math.floor(Math.log10(x));
        double f = x / Math.pow(10, exp);
        double nf;
        if (round) nf = (f < 1.5) ? 1 : (f < 3) ? 2 : (f < 7) ? 5 : 10;
        else       nf = (f <= 1)  ? 1 : (f <= 2) ? 2 : (f <= 5) ? 5 : 10;
        return nf * Math.pow(10, exp);
    }
    private static class PlotArea {
        final int L,B,PW,PH,W,H;
        PlotArea(int L,int B,int PW,int PH,int W,int H){this.L=L;this.B=B;this.PW=PW;this.PH=PH;this.W=W;this.H=H;}
        int x0(){ return L; }
        int y0(){ return H - B; }
        int top(){ return H - B - PH; }
        Rectangle clip(){ return new Rectangle(L, top(), PW, PH); }
    }
    private static PlotArea drawAxesWithTicks(Graphics2D g, int W, int H,
                                              String title, String xLabel, String yLabel,
                                              double xMin, double xMax, double yMin, double yMax) {

        int topMargin = 70;
        int L = 90, B = 90;
        int PW = W - 2*L;
        int PH = H - B - topMargin;


        g.setColor(Color.WHITE); g.fillRect(0, 0, W, H);

        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);

        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.BOLD, 18));
        FontMetrics fm = g.getFontMetrics();
        int titleW = fm.stringWidth(title);
        g.drawString(title, (W - titleW) / 2, 30);

        g.setFont(new Font("SansSerif", Font.PLAIN, 13));
        g.drawLine(L, H - B, L + PW, H - B);         // eje X
        g.drawLine(L, H - B, L, H - B - PH);         // eje Y

        String xLab = xLabel == null ? "" : xLabel;
        String yLab = yLabel == null ? "" : yLabel;
        g.drawString(xLab, L + PW/2 - g.getFontMetrics().stringWidth(xLab)/2, H - 25);
        g.rotate(-Math.PI / 2);
        g.drawString(yLab, -(H - B - PH/2 + g.getFontMetrics().stringWidth(yLab)/2), 25);
        g.rotate(Math.PI / 2);

        NiceTicks xt = niceTicks(xMin, xMax, 8);
        NiceTicks yt = niceTicks(yMin, yMax, 8);

        g.setColor(new Color(230,230,230));
        for (double xv = xt.min; xv <= xt.max + 1e-9; xv += xt.step) {
            int x = L + (int) ((xv - xt.min) / (xt.max - xt.min) * PW);
            g.drawLine(x, H - B, x, H - B - PH);
        }
        for (double yv = yt.min; yv <= yt.max + 1e-9; yv += yt.step) {
            int y = H - B - (int) ((yv - yt.min) / (yt.max - yt.min) * PH);
            g.drawLine(L, y, L + PW, y);
        }

        g.setColor(Color.BLACK);
        DecimalFormat dfX = new DecimalFormat("0.###");
        DecimalFormat dfY = new DecimalFormat("0.###");

        for (double xv = xt.min; xv <= xt.max + 1e-9; xv += xt.step) {
            int x = L + (int) ((xv - xt.min) / (xt.max - xt.min) * PW);
            g.drawLine(x, H - B, x, H - B + 5);
            String s = dfX.format(xv);
            int w = g.getFontMetrics().stringWidth(s);
            g.drawString(s, x - w/2, H - B + 20);
        }
        for (double yv = yt.min; yv <= yt.max + 1e-9; yv += yt.step) {
            int y = H - B - (int) ((yv - yt.min) / (yt.max - yt.min) * PH);
            g.drawLine(L - 5, y, L, y);
            String s = dfY.format(yv);
            int w = g.getFontMetrics().stringWidth(s);
            g.drawString(s, L - 10 - w, y + 5);
        }

        PlotArea area = new PlotArea(L, B, PW, PH, W, H);
        g.setClip(area.clip());
        return area;
    }

    private static void plotThroughput(Map<Integer, Double> data, Path outPng) throws IOException {
        int W = 900, H = 600;
        BufferedImage img = new BufferedImage(W, H, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();

        List<Integer> xs = new ArrayList<>(data.keySet());
        Collections.sort(xs);
        double minX = xs.get(0), maxX = xs.get(xs.size()-1);
        double minY = 0.0, maxY = data.values().stream().mapToDouble(Double::doubleValue).max().orElse(1.0);
        maxY *= 1.05;

        PlotArea area = drawAxesWithTicks(g, W, H,
                "Throughput vs Threads — " + DEVICE, "Threads", "Throughput (ops/sec)",
                minX, maxX, minY, maxY);

        g.setColor(new Color(30, 144, 255));
        int prevX = -1, prevY = -1;
        for (int i = 0; i < xs.size(); i++) {
            double xv = xs.get(i);
            double yv = data.get(xs.get(i));
            int x = area.L + (int) ((xv - minX) / (maxX - minX) * area.PW);
            int y = area.H - area.B - (int) ((yv - minY) / (maxY - minY) * area.PH);
            g.fillOval(x - 4, y - 4, 8, 8);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }

        g.setClip(null);
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
            if (b < 0) b = 0;
            hist[b]++;
        }
        int maxCount = Arrays.stream(hist).max().orElse(1);

        int W = 900, H = 600;
        BufferedImage img = new BufferedImage(W, H, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();

        PlotArea area = drawAxesWithTicks(g, W, H,
                "Latency Histogram (ms) — " + DEVICE, "Latency (ms)", "Frequency",
                min, max, 0, maxCount * 1.05);

        g.setColor(new Color(34, 139, 34));
        int barW = Math.max(1, area.PW / bins);
        for (int i = 0; i < bins; i++) {
            double x0 = min + i * (max - min) / bins;
            double x1 = min + (i + 1) * (max - min) / bins;
            int x = area.L + (int) ((x0 - min) / (max - min) * area.PW);
            int h = (int) ((hist[i] / (double) (maxCount * 1.05)) * area.PH);
            int y = area.H - area.B - h;
            g.fillRect(x, y, Math.max(1, barW - 2), h);
        }

        g.setClip(null);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    private static void plotCpuSeries(List<long[]> series, Path outPng) throws IOException {
        if (series.isEmpty()) return;
        int W = 900, H = 600;
        BufferedImage img = new BufferedImage(W, H, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();

        long t0 = series.get(0)[0], tN = series.get(series.size() - 1)[0];
        double minX = 0, maxX = Math.max(1, tN - t0);
        double minY = 0, maxY = 100;

        PlotArea area = drawAxesWithTicks(g, W, H,
                "CPU (%) over Time — " + DEVICE, "Time (ms)", "CPU (%)",
                minX, maxX, minY, maxY);

        g.setColor(new Color(75, 0, 130));
        int prevX = -1, prevY = -1;
        for (long[] r : series) {
            long ts = r[0], cpu = r[1];
            int x = area.L + (int) (((ts - t0) - minX) / (maxX - minX) * area.PW);
            int y = area.H - area.B - (int) ((cpu - minY) / (maxY - minY) * area.PH);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }

        g.setClip(null);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    private static void plotMemSeries(List<long[]> series, Path outPng) throws IOException {
        if (series.isEmpty()) return;
        int W = 900, H = 600;
        BufferedImage img = new BufferedImage(W, H, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();

        long t0 = series.get(0)[0], tN = series.get(series.size() - 1)[0];
        double minX = 0, maxX = Math.max(1, tN - t0);
        long maxUsedRaw = series.stream().mapToLong(r -> r[2]).max().orElse(1);
        double minY = 0, maxY = maxUsedRaw * 1.05;

        PlotArea area = drawAxesWithTicks(g, W, H,
                "Memory (MB) over Time — " + DEVICE, "Time (ms)", "Used Memory (MB)",
                minX, maxX, minY, maxY);

        g.setColor(new Color(220, 20, 60));
        int prevX = -1, prevY = -1;
        for (long[] r : series) {
            long ts = r[0], used = r[2];
            int x = area.L + (int) (((ts - t0) - minX) / (maxX - minX) * area.PW);
            int y = area.H - area.B - (int) ((used - minY) / (maxY - minY) * area.PH);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }

        g.setClip(null);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }
}
