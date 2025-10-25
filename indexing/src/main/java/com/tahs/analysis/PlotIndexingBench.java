package com.tahs.analysis;

import java.nio.file.*;

public class PlotIndexingBench {

    public static void main(String[] args) throws Exception {
        Path csvPath = Path.of(args.length > 0 ? args[0] : "benchmarking_results/indexing_data.csv");
        Path outDir  = Path.of("benchmarking_results/indexing/plots");
        Path sumDir  = Path.of("benchmarking_results/indexing");
        Files.createDirectories(outDir);
        Files.createDirectories(sumDir);

        System.out.println("Folder for indexing data plots: " + outDir.toAbsolutePath());
    }
}