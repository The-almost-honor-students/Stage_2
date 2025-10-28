package com.tahs.benchmark;

import com.tahs.clients.IngestionClient;
import com.tahs.clients.IndexingClient;
import com.tahs.clients.SearchClient;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@Fork(1)
@State(Scope.Benchmark)

public class IntegrationBenchmark {

    @Param({"1-500"})
    public String bookIds;

    @Param({"book,science,art,war,life,death"})
    public String queryTerms;

    @Param({"10"})
    public int timeoutSec;

    private IngestionClient ingestionClient;
    private IndexingClient indexingClient;
    private SearchClient searchClient;
    private List<Integer> ids;
    private List<String> terms;
    private AtomicInteger rrId;
    private AtomicInteger rrTerm;
    private static final Path METRICS_CSV = Paths.get("benchmarking_results/integration/system_metrics.csv");

    @Setup(Level.Trial)
    public void setup() {
        var httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeoutSec)).build();
        ingestionClient = new IngestionClient(httpClient);
        indexingClient = new IndexingClient(httpClient);
        searchClient = new SearchClient(httpClient);
        ids = parseIds(bookIds);
        terms = parseTerms(queryTerms);
        rrId = new AtomicInteger(0);
        rrTerm = new AtomicInteger(0);
        try {
            Files.createDirectories(METRICS_CSV.getParent());
            if (!Files.exists(METRICS_CSV))
                Files.writeString(METRICS_CSV, "timestamp_ms,phase,cpu_percent,used_memory_mb,total_memory_mb\n");
        } catch (IOException ignored) {}
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void ingestionThroughput(Blackhole bh) throws Exception {
        int id = nextId();
        var resp = ingestionClient.downloadBook(String.valueOf(id));
        recordCpuAndMemory("ingestion");
        bh.consume(resp);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void indexingLatency(Blackhole bh) throws Exception {
        int id = nextId();
        var resp = indexingClient.updateIndexForBook(String.valueOf(id));
        recordCpuAndMemory("indexing");
        bh.consume(resp);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public void queryResponseTime(Blackhole bh) throws Exception {
        String q = nextTerm();
        String resp = searchClient.search(q);
        recordCpuAndMemory("search");
        bh.consume(resp);
    }

    private void recordCpuAndMemory(String phase) {
        try {
            OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            double cpuLoad = -1.0;
            if (os instanceof com.sun.management.OperatingSystemMXBean osm)
                cpuLoad = osm.getProcessCpuLoad() * 100.0;
            long usedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long totalMem = Runtime.getRuntime().maxMemory();
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(METRICS_CSV, StandardCharsets.UTF_8, StandardOpenOption.APPEND))) {
                w.printf(Locale.US, "%d,%s,%.2f,%.2f,%.2f%n", System.currentTimeMillis(), phase, cpuLoad, usedMem / 1e6, totalMem / 1e6);
            }
        } catch (IOException ignored) {}
    }

    private int nextId() {
        int i = Math.floorMod(rrId.getAndIncrement(), ids.size());
        return ids.get(i);
    }

    private String nextTerm() {
        int i = Math.floorMod(rrTerm.getAndIncrement(), terms.size());
        return terms.get(i);
    }

    private static List<Integer> parseIds(String spec) {
        spec = spec.trim();
        if (spec.matches("\\d+\\s*-\\s*\\d+")) {
            String[] p = spec.split("-");
            int a = Integer.parseInt(p[0].trim());
            int b = Integer.parseInt(p[1].trim());
            if (a > b) { int t = a; a = b; b = t; }
            List<Integer> out = new ArrayList<>(b - a + 1);
            for (int i = a; i <= b; i++) out.add(i);
            return out;
        }
        return Arrays.stream(spec.split(",")).map(String::trim).filter(s -> !s.isEmpty()).map(Integer::parseInt).collect(Collectors.toList());
    }

    private static List<String> parseTerms(String csv) {
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    public static void main(String[] args) throws Exception {
        final String BENCH = "integration";
        Path base = Path.of("benchmarking_results").resolve(BENCH).resolve("data");
        Files.createDirectories(base);
        Path mergedAgg = base.resolve(BENCH + "_agg.csv");
        Path mergedIters = base.resolve(BENCH + "_data.csv");

        try (PrintWriter all = new PrintWriter(Files.newBufferedWriter(mergedIters, StandardCharsets.UTF_8))) {
            all.println("threads,phase,iteration,value,unit,mode,benchmark");
        }

        int[] threadSweep = {1, 2, 4, 8};
        List<Path> partialAgg = new ArrayList<>();

        for (int t : threadSweep) {
            Path aggCsv = base.resolve("jmh_t" + t + ".csv");
            Path rawLog = base.resolve(BENCH + "_t" + t + "_raw.txt");
            Path iterCsv = base.resolve(BENCH + "_iterations_t" + t + ".csv");

            Options opt = new OptionsBuilder()
                    .include(IntegrationBenchmark.class.getSimpleName())
                    .warmupIterations(3)
                    .measurementIterations(10)
                    .forks(1)
                    .threads(t)
                    .verbosity(VerboseMode.EXTRA)
                    .timeUnit(TimeUnit.MILLISECONDS)
                    .result(aggCsv.toString())
                    .resultFormat(ResultFormatType.CSV)
                    .output(rawLog.toString())
                    .build();

            new Runner(opt).run();

            writeIterCsvHeader(iterCsv);
            parseRawToIterationsCsv(rawLog, iterCsv, t);
            appendCsvWithoutHeader(iterCsv, mergedIters);
            partialAgg.add(aggCsv);
        }

        mergeCsvWithHeader(partialAgg, mergedAgg);
        System.out.println("Aggregated CSV: " + mergedAgg.toAbsolutePath());
        System.out.println("Merged iterations CSV: " + mergedIters.toAbsolutePath());
        System.out.println("System metrics CSV: " + METRICS_CSV.toAbsolutePath());
    }

    private static void writeIterCsvHeader(Path csv) throws IOException {
        if (!Files.exists(csv) || Files.size(csv) == 0) {
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(csv, StandardCharsets.UTF_8))) {
                w.println("threads,phase,iteration,value,unit,mode,benchmark");
            }
        }
    }

    private static void appendCsvWithoutHeader(Path src, Path dst) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(src, StandardCharsets.UTF_8);
             PrintWriter out = new PrintWriter(Files.newBufferedWriter(dst, StandardCharsets.UTF_8, StandardOpenOption.APPEND))) {
            String line; boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; }
                if (!line.isBlank()) out.println(line);
            }
        }
    }

    private static void parseRawToIterationsCsv(Path rawLog, Path outCsv, int threads) throws IOException {
        Pattern warmup = Pattern.compile("^#\\s*Warmup Iteration\\s+(\\d+):\\s+([0-9.,]+)\\s+(.+)$");
        Pattern meas = Pattern.compile("^Iteration\\s+(\\d+):\\s+([0-9.,]+)\\s+(.+)$");
        Pattern bench = Pattern.compile("^# Benchmark:\\s*(.+)$");
        Pattern mode = Pattern.compile("^# Mode:\\s*(.+)$");

        String currentBenchmark = "";
        String currentMode = "";

        try (BufferedReader br = Files.newBufferedReader(rawLog, StandardCharsets.UTF_8);
             PrintWriter w = new PrintWriter(Files.newBufferedWriter(outCsv, StandardCharsets.UTF_8, StandardOpenOption.APPEND))) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher mb = bench.matcher(line);
                if (mb.find()) { currentBenchmark = mb.group(1).trim(); continue; }
                Matcher mmode = mode.matcher(line);
                if (mmode.find()) { currentMode = mmode.group(1).trim().toLowerCase(Locale.ROOT); continue; }
                Matcher mw = warmup.matcher(line);
                if (mw.find()) {
                    int it = Integer.parseInt(mw.group(1));
                    double val = parseLocaleNumber(mw.group(2));
                    String unit = mw.group(3).trim();
                    String resolvedMode = inferMode(unit, currentMode);
                    w.printf(Locale.US, "%d,%s,%d,%.9f,%s,%s,%s%n", threads, "warmup", it, val, unit, resolvedMode, currentBenchmark);
                    continue;
                }
                Matcher mi = meas.matcher(line);
                if (mi.find()) {
                    int it = Integer.parseInt(mi.group(1));
                    double val = parseLocaleNumber(mi.group(2));
                    String unit = mi.group(3).trim();
                    String resolvedMode = inferMode(unit, currentMode);
                    w.printf(Locale.US, "%d,%s,%d,%.9f,%s,%s,%s%n", threads, "iteration", it, val, unit, resolvedMode, currentBenchmark);
                }
            }
        }
    }

    private static String inferMode(String unit, String modeFromHeader) {
        String u = unit.toLowerCase(Locale.ROOT);
        if (u.contains("ops/s")) return "thrpt";
        if (u.contains("/op")) return "avgt";
        return (modeFromHeader == null || modeFromHeader.isBlank()) ? "unknown" : modeFromHeader;
    }

    private static double parseLocaleNumber(String s) {
        return Double.parseDouble(s.replace(",", "").trim());
    }

    private static void mergeCsvWithHeader(List<Path> inputs, Path output) throws IOException {
        if (inputs.isEmpty()) return;
        List<String> header = Files.readAllLines(inputs.get(0), StandardCharsets.UTF_8);
        if (header.isEmpty()) return;
        List<String> out = new ArrayList<>();
        out.add(header.get(0));
        for (Path in : inputs) {
            List<String> lines = Files.readAllLines(in, StandardCharsets.UTF_8);
            for (int i = 1; i < lines.size(); i++) out.add(lines.get(i));
        }
        Files.write(output, out, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
