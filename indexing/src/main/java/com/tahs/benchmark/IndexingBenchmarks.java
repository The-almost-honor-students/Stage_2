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
import java.nio.file.Files;
import java.nio.file.Path;
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

}
