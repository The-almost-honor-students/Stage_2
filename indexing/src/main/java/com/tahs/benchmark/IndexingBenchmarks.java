package com.tahs.benchmark;

import org.openjdk.jmh.annotations.*;
import java.util.concurrent.TimeUnit;


@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
@State(Scope.Benchmark)

public class IndexingBenchmarks {


}
