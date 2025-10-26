package com.tahs.benchmark;

import com.tahs.application.ports.DatalakeRepository;
import com.tahs.application.usecase.IngestionService;
import com.tahs.infrastructure.FsDatalakeRepository;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
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
public class IngestionBenchmarks {

    @Param({"staging/downloads"})
    public String stagingDir;

    @Param({"datalake"})
    public String datalakeDir;

    @Param({"1-500"})
    public String bookIds;

    @Param({"70000"})
    public int totalBooks;

    @Param({"10"})
    public int maxRetries;

    private IngestionService ingestion;
    private List<Integer> candidates;
    private AtomicInteger rr;

    @Setup(Level.Trial)
    public void setup() {
        rr = new AtomicInteger(0);
        DatalakeRepository repo = new FsDatalakeRepository(datalakeDir);
        ingestion = new IngestionService(repo, Paths.get(stagingDir), totalBooks, maxRetries);
        candidates = parseIds(bookIds);
        ensureDirs();
        purgeStaging();
    }

    @TearDown(Level.Iteration)
    public void clearBetweenIterations() {
        purgeStaging();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput) // books/s
    public void ingestThroughput_roundRobin(Blackhole bh) {
        int id = nextId();
        boolean ok = ingestion.ingestOne(id, LocalDateTime.now());
        bh.consume(ok);
    }

    private int nextId() {
        int idx = Math.floorMod(rr.getAndIncrement(), candidates.size());
        return candidates.get(idx);
    }

    private List<Integer> parseIds(String spec) {
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
        return Arrays.stream(spec.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }

    private void ensureDirs() {
        try {
            Files.createDirectories(Paths.get(stagingDir));
            Files.createDirectories(Paths.get(datalakeDir));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void purgeStaging() {
        try (Stream<Path> s = Files.list(Paths.get(stagingDir))) {
            s.filter(Files::isRegularFile)
                    .filter(p -> {
                        String n = p.getFileName().toString();
                        return n.endsWith("_body.txt") || n.endsWith("_header.txt");
                    })
                    .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
        } catch (IOException ignored) {}
    }

    public static void main(String[] args) throws Exception {
        if (args != null && args.length > 0) {
            org.openjdk.jmh.Main.main(args);
            return;
        }

        final String BENCH = "ingestion";
        Path base = Path.of("benchmarking_results").resolve(BENCH);
        Path dataDir = base.resolve("data");
        Files.createDirectories(dataDir);

        Path mergedAgg   = dataDir.resolve(BENCH + "_agg.csv");
        Path mergedIters = dataDir.resolve(BENCH + "_data.csv");
        writeIterCsvHeader(mergedIters);

        List<Path> partialAgg = new ArrayList<>();
        int[] threadSweep = {1, 2, 4, 8};

        for (int t : threadSweep) {
            Path aggCsv  = dataDir.resolve("ingestion_t" + t + ".csv");
            Path rawLog  = dataDir.resolve(BENCH + "_t" + t + "_raw.txt");
            Path iterCsv = dataDir.resolve(BENCH + "_iterations_t" + t + ".csv");

            Options opt = new OptionsBuilder()
                    .include(IngestionBenchmarks.class.getSimpleName())
                    .warmupIterations(5)
                    .measurementIterations(10)
                    .forks(1)
                    .threads(t)
                    .verbosity(VerboseMode.EXTRA)
                    .timeUnit(TimeUnit.SECONDS)
                    .result(aggCsv.toString())
                    .resultFormat(ResultFormatType.CSV)
                    .build();

            PrintStream originalOut = System.out;
            try (FileOutputStream fos = new FileOutputStream(rawLog.toFile());
                 PrintStream fileOut = new PrintStream(fos, true, StandardCharsets.UTF_8);
                 PrintStream dual = new PrintStream(new DualOutputStream(originalOut, fileOut), true, StandardCharsets.UTF_8)) {

                System.setOut(dual);
                System.out.printf("%n=== Running %s benchmark with %d threads ===%n", BENCH, t);
                Collection<RunResult> _ignore = new Runner(opt).run();
                System.out.printf("=== Finished %s benchmark with %d threads ===%n", BENCH, t);
            } finally {
                System.setOut(originalOut);
            }

            writeIterCsvHeader(iterCsv);
            int written = parseRawToIterationsCsv(rawLog, iterCsv, t);

            if (written <= 0) {
                try { Files.deleteIfExists(iterCsv); } catch (IOException ignored) {}
            } else {
                appendCsvWithoutHeader(iterCsv, mergedIters);
            }
            if (fileHasMoreThanHeader(aggCsv)) {
                partialAgg.add(aggCsv);
            } else {
                try { Files.deleteIfExists(aggCsv); } catch (IOException ignored) {}
            }
        }

        mergeCsvWithHeader(partialAgg, mergedAgg);

        if (!fileHasMoreThanHeader(mergedIters)) {
            try { Files.deleteIfExists(mergedIters); } catch (IOException ignored) {}
            System.out.println("No iteration data to merge. Removed: " + mergedIters.toAbsolutePath());
        }

        System.out.println("Aggregated CSV: " + mergedAgg.toAbsolutePath());
        System.out.println("Merged iterations CSV: " + mergedIters.toAbsolutePath());
        System.out.println("Per-thread iteration CSVs folder: " + dataDir.toAbsolutePath());
    }

    private static final class DualOutputStream extends OutputStream {
        private final PrintStream a, b;
        DualOutputStream(PrintStream a, PrintStream b) { this.a = a; this.b = b; }
        @Override public void write(int i) { a.write(i); b.write(i); }
        @Override public void write(byte[] buf, int off, int len) { a.write(buf, off, len); b.write(buf, off, len); }
        @Override public void flush() { a.flush(); b.flush(); }
        @Override public void close() { a.flush(); b.flush(); }
    }

    private static void writeIterCsvHeader(Path csv) throws IOException {
        if (!Files.exists(csv) || Files.size(csv) == 0) {
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(csv, StandardCharsets.UTF_8))) {
                w.println("threads,phase,iteration,value,unit,mode,benchmark");
            }
        }
    }

    private static boolean fileHasMoreThanHeader(Path p) {
        try {
            if (!Files.exists(p)) return false;
            long lines = Files.lines(p).limit(2).count();
            return lines >= 2;
        } catch (IOException e) {
            return false;
        }
    }

    private static void appendCsvWithoutHeader(Path src, Path dst) throws IOException {
        if (!Files.exists(src)) return;
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

    private static int parseRawToIterationsCsv(Path rawLog, Path outCsv, int threads) throws IOException {
        Pattern warmup = Pattern.compile("^#\\s*Warmup\\s+Iteration\\s+(\\d+)\\s*:\\s*([0-9.,]+)(?:\\s*[±+\\-–—]\\s*[0-9.,]+)?\\s*(\\S.*)$");
        Pattern meas   = Pattern.compile("^Iteration\\s+(\\d+)\\s*:\\s*([0-9.,]+)(?:\\s*[±+\\-–—]\\s*[0-9.,]+)?\\s*(\\S.*)$");
        Pattern bench  = Pattern.compile("^#\\s*Benchmark:\\s*(.+)$");
        Pattern mode   = Pattern.compile("^#\\s*Mode:\\s*(.+)$");

        String currentBenchmark = "";
        String currentMode = "";
        int written = 0;

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
                    written++;
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
                    written++;
                }
            }
        }
        return written;
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
        List<Path> usable = inputs.stream().filter(IngestionBenchmarks::fileHasMoreThanHeader).collect(Collectors.toList());
        if (usable.isEmpty()) {
            try { Files.deleteIfExists(output); } catch (IOException ignored) {}
            return;
        }
        List<String> header = Files.readAllLines(usable.get(0), StandardCharsets.UTF_8);
        if (header.isEmpty()) return;
        List<String> out = new ArrayList<>();
        out.add(header.get(0));
        for (Path in : usable) {
            List<String> lines = Files.readAllLines(in, StandardCharsets.UTF_8);
            for (int i = 1; i < lines.size(); i++) out.add(lines.get(i));
        }
        Files.write(output, out, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
