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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    @BenchmarkMode(Mode.Throughput) // -> ops/s (books/s)
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

        Path mergedAgg = dataDir.resolve(BENCH + "_agg.csv");
        List<Path> partialAgg = new ArrayList<>();

        int[] threadSweep = {1, 2, 4, 8};

        for (int t : threadSweep) {
            Path aggCsv  = dataDir.resolve("ingestion_t" + t + ".csv");
            Path rawLog  = dataDir.resolve(BENCH + "_t" + t + "_raw.txt");

            Options opt = new OptionsBuilder()
                    .include(IngestionBenchmarks.class.getSimpleName())
                    .warmupIterations(5)
                    .measurementIterations(10)
                    .forks(1)
                    .threads(t)
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

            partialAgg.add(aggCsv);
        }

        mergeCsvWithHeader(partialAgg, mergedAgg);
        System.out.println("Aggregated CSV: " + mergedAgg.toAbsolutePath());
        System.out.println("Data folder: " + dataDir.toAbsolutePath());
    }

    private static final class DualOutputStream extends OutputStream {
        private final PrintStream a, b;
        DualOutputStream(PrintStream a, PrintStream b) { this.a = a; this.b = b; }
        @Override public void write(int i) { a.write(i); b.write(i); }
        @Override public void write(byte[] buf, int off, int len) { a.write(buf, off, len); }
        @Override public void flush() { a.flush(); b.flush(); }
        @Override public void close() { a.flush(); b.flush(); }
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
