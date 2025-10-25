package com.tahs.benchmark;

import com.tahs.domain.Book;
import com.tahs.infrastructure.serialization.books.GutenbergHeaderSerializer;
import com.tahs.infrastructure.serialization.books.TextTokenizer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
@State(Scope.Benchmark)

public class IndexingBenchmarks {
    @Param({"datalake"})
    public String datalakeDir;

    @Param({"1342"})
    public String bookId;

    private List<String> availableIds;
    private Map<String, Set<String>> invertedIndex;
    private Map<String, Book> metadataRepo;
    private GutenbergHeaderSerializer headerSerializer;
    private AtomicInteger rr;

    @Setup(Level.Trial)
    public void setup() {
        invertedIndex = new ConcurrentHashMap<>(1 << 14);
        metadataRepo = new ConcurrentHashMap<>();
        headerSerializer = new GutenbergHeaderSerializer();
        rr = new AtomicInteger(0);
        Path root = Path.of(datalakeDir).toAbsolutePath().normalize();
        if (!Files.exists(root)) throw new IllegalStateException("Datalake directory not found: " + root);
        availableIds = discoverIdsWithHeaderAndBody(root);
        if (availableIds.isEmpty()) throw new IllegalStateException("No valid <id>.header.txt + <id>.body.txt pairs found in: " + root);
        if (!availableIds.contains(bookId)) bookId = availableIds.get(0);
    }

    @TearDown(Level.Iteration)
    public void clearBetweenIterations() {
        invertedIndex.clear();
        metadataRepo.clear();
        rr.set(0);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void indexLatency_perBook(Blackhole bh) throws IOException {
        String headerPath = findInDatalake(bookId, "header");
        Book book = headerSerializer.deserialize(headerPath);
        metadataRepo.put(bookId, book);
        String bodyPath = findInDatalake(bookId, "body");
        String text = headerSerializer.readFile(bodyPath);
        Set<String> terms = TextTokenizer.extractTerms(text);
        indexBookInMemory(bookId, terms);
        bh.consume(invertedIndex.size());
        bh.consume(invertedIndex.getOrDefault("indexing", Set.of()).size());
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void throughputIndexing_roundRobin(Blackhole bh) throws IOException {
        String id = nextId();
        String headerPath = findInDatalake(id, "header");
        Book book = headerSerializer.deserialize(headerPath);
        metadataRepo.put(id, book);
        String bodyPath = findInDatalake(id, "body");
        String text = headerSerializer.readFile(bodyPath);
        Set<String> terms = TextTokenizer.extractTerms(text);
        indexBookInMemory(id, terms);
        bh.consume(book.getAuthor());
        bh.consume(invertedIndex.getOrDefault("indexing", Set.of()).size());
    }

    private List<String> discoverIdsWithHeaderAndBody(Path root) {
        try (Stream<Path> s = Files.find(root, 5, (p, a) -> a.isRegularFile() && p.getFileName().toString().endsWith(".txt"))) {
            Map<String, Set<String>> filesById = new HashMap<>();
            s.forEach(p -> {
                String[] parts = p.getFileName().toString().split("\\.");
                if (parts.length >= 3) filesById.computeIfAbsent(parts[0], k -> new HashSet<>()).add(parts[1]);
            });
            return filesById.entrySet().stream()
                    .filter(e -> e.getValue().contains("header") && e.getValue().contains("body"))
                    .map(Map.Entry::getKey)
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String nextId() {
        int idx = Math.floorMod(rr.getAndIncrement(), availableIds.size());
        return availableIds.get(idx);
    }

    private void indexBookInMemory(String id, Set<String> terms) {
        for (String term : terms) {
            invertedIndex
                    .computeIfAbsent(term, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                    .add(id);
        }
    }

    private String findInDatalake(String id, String section) {
        String fileName = id + "." + section + ".txt";
        Path root = Path.of(datalakeDir).toAbsolutePath().normalize();
        try (Stream<Path> s = Files.find(root, 5, (p, attrs) -> attrs.isRegularFile() && p.getFileName().toString().equals(fileName))) {
            return s.findFirst().map(Path::toString)
                    .orElseThrow(() -> new IllegalStateException("File not found: " + fileName + " in " + root));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args != null && args.length > 0) {
            org.openjdk.jmh.Main.main(args);
            return;
        }

        Path base = Path.of("benchmarking_results").resolve("indexing").resolve("data");
        Files.createDirectories(base);
        Path merged = base.resolve("indexing_data.csv");

        int[] threadSweep = {1, 2, 4, 8};
        List<Path> partials = new ArrayList<>();

        for (int t : threadSweep) {
            Path out = base.resolve("jmh_t" + t + ".csv");
            Options opt = new OptionsBuilder()
                    .include(IndexingBenchmarks.class.getSimpleName())
                    .warmupIterations(5)
                    .measurementIterations(10)
                    .forks(1)
                    .timeUnit(TimeUnit.SECONDS)
                    .threads(t)
                    .result(out.toString())
                    .resultFormat(ResultFormatType.CSV)
                    .build();
            new Runner(opt).run();
            partials.add(out);
        }

        mergeCsv(partials, merged);
        for (Path p : partials) {
            try { Files.deleteIfExists(p); } catch (Exception ignored) {}
        }
        System.out.println("CSV merged: " + merged.toAbsolutePath());
    }

    private static void mergeCsv(List<Path> inputs, Path output) throws IOException {
        if (inputs.isEmpty()) throw new IOException("No CSV inputs to merge");
        List<String> header = Files.readAllLines(inputs.get(0), StandardCharsets.UTF_8);
        if (header.isEmpty()) throw new IOException("First CSV has no header: " + inputs.get(0));

        List<String> out = new ArrayList<>();
        out.add(header.get(0)); // header
        for (Path in : inputs) {
            List<String> lines = Files.readAllLines(in, StandardCharsets.UTF_8);
            for (int i = 1; i < lines.size(); i++) {
                out.add(lines.get(i));
            }
        }
        Files.write(output, out, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
