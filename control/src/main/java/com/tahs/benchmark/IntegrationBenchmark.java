package com.tahs.benchmark;
import com.tahs.clients.IndexingClient;
import com.tahs.clients.IngestionClient;
import com.tahs.clients.SearchClient;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
@State(Scope.Benchmark)

public class IntegrationBenchmark {
    private AtomicInteger rr;


    @Setup(Level.Trial)
    public void setup() {
        var httpClient = HttpClient.newHttpClient();
        var ingestionClient = new IngestionClient(httpClient);
        var indexingClient = new IndexingClient(httpClient);
        var searchClient = new SearchClient(httpClient);
    }

    @TearDown(Level.Iteration)
    public void clearBetweenIterations() {

    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void indexLatency_perBook(Blackhole bh) throws IOException {

    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void throughputIndexing_roundRobin(Blackhole bh) throws IOException {

    }



    public static void main(String[] args) throws Exception {
        if (args != null && args.length > 0) {
            org.openjdk.jmh.Main.main(args);
            return;
        }

        final String BENCH = "indexing";
        Path base = Path.of("benchmarking_results").resolve(BENCH).resolve("data");
        Files.createDirectories(base);

        Path mergedAgg   = base.resolve(BENCH + "_agg.csv");
        Path mergedIters = base.resolve(BENCH + "_data.csv");
        try (PrintWriter all = new PrintWriter(Files.newBufferedWriter(mergedIters, StandardCharsets.UTF_8))) {
            all.println("threads,phase,iteration,value,unit,mode,benchmark");
        }

        int[] threadSweep = {1, 2, 4, 8};
        List<Path> partialAgg = new ArrayList<>();

        for (int t : threadSweep) {
            Path aggCsv  = base.resolve("jmh_t" + t + ".csv");
            Path rawLog  = base.resolve(BENCH + "_t" + t + "_raw.txt");
            Path iterCsv = base.resolve(BENCH + "_iterations_t" + t + ".csv");

            Options opt = new OptionsBuilder()
                    .include(IndexingBenchmarks.class.getSimpleName())
                    .warmupIterations(5)
                    .measurementIterations(10)
                    .forks(1)
                    .threads(t)
                    .timeUnit(TimeUnit.SECONDS)
                    .result(aggCsv.toString())
                    .resultFormat(ResultFormatType.CSV)
                    .output(rawLog.toString())
                    .build();

            Collection<RunResult> _ignore = new Runner(opt).run();

            writeIterCsvHeader(iterCsv);
            parseRawToIterationsCsv(rawLog, iterCsv, t);
            appendCsvWithoutHeader(iterCsv, mergedIters);

            partialAgg.add(aggCsv);
        }

        mergeCsvWithHeader(partialAgg, mergedAgg);
        for (Path p : partialAgg) { try { Files.deleteIfExists(p); } catch (Exception ignored) {} }

        System.out.println("Aggregated CSV: " + mergedAgg.toAbsolutePath());
        System.out.println("Per-thread iteration CSVs: " + base.toAbsolutePath());
        System.out.println("Merged iterations CSV: " + mergedIters.toAbsolutePath());
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
            String line;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; }
                if (!line.isBlank()) out.println(line);
            }
        }
    }

    private static void parseRawToIterationsCsv(Path rawLog, Path outCsv, int threads) throws IOException {
        Pattern warmup = Pattern.compile("^#\\s*Warmup Iteration\\s+(\\d+):\\s+([0-9.,]+)\\s+(.+)$");
        Pattern meas   = Pattern.compile("^Iteration\\s+(\\d+):\\s+([0-9.,]+)\\s+(.+)$");
        Pattern bench  = Pattern.compile("^# Benchmark:\\s*(.+)$");
        Pattern mode   = Pattern.compile("^# Mode:\\s*(.+)$");

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
                    w.printf(Locale.US, "%d,%s,%d,%.9f,%s,%s,%s%n",
                            threads, "warmup", it, val, unit, resolvedMode, currentBenchmark);
                    continue;
                }

                Matcher mi = meas.matcher(line);
                if (mi.find()) {
                    int it = Integer.parseInt(mi.group(1));
                    double val = parseLocaleNumber(mi.group(2));
                    String unit = mi.group(3).trim();
                    String resolvedMode = inferMode(unit, currentMode);
                    w.printf(Locale.US, "%d,%s,%d,%.9f,%s,%s,%s%n",
                            threads, "iteration", it, val, unit, resolvedMode, currentBenchmark);
                }
            }
        }
    }

    private static String inferMode(String unit, String modeFromHeader) {
        String u = unit.toLowerCase(Locale.ROOT);
        if (u.contains("ops/s")) return "thrpt";
        if (u.contains("/op"))  return "avgt";
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
