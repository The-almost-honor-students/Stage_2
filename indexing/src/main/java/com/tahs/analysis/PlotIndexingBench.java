package com.tahs.analysis;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.Reader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class PlotIndexingBench {

    public static void main(String[] args) throws Exception {
        Path csvPath = Path.of(args.length > 0 ? args[0] : "benchmarking_results/indexing_data.csv");
        Path outDir  = Path.of("benchmarking_results/indexing/plots");
        Path sumDir  = Path.of("benchmarking_results/indexing");
        Files.createDirectories(outDir);
        Files.createDirectories(sumDir);

        List<Row> rows = load(csvPath);
        System.out.println("Loaded " + rows.size() + " records.");
        System.out.println("Output folders ready: " + outDir.toAbsolutePath());
    }

    static final class Row {
        final String benchmark;
        final String mode;
        final int threads;
        final double scoreSecPerOp;
        final double scoreOpsPerSec;

        Row(String b, String m, int t, double sSec, double sOps) {
            this.benchmark = b;
            this.mode = m;
            this.threads = t;
            this.scoreSecPerOp = sSec;
            this.scoreOpsPerSec = sOps;
        }
    }

    static List<Row> load(Path csv) throws IOException {
        if (!Files.exists(csv)) throw new IOException("CSV not found: " + csv.toAbsolutePath());
        try (Reader r = Files.newBufferedReader(csv, StandardCharsets.UTF_8);
             CSVParser p = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim().parse(r)) {

            Map<String, Integer> H = lowerHeader(p.getHeaderMap());
            List<Row> out = new ArrayList<>();

            for (CSVRecord rec : p) {
                String benchmark = get(rec, H, "benchmark");
                String mode = get(rec, H, "mode");
                String units = get(rec, H, "units", "unit");
                int threads = parseInt(get(rec, H, "threads", "param:threads", "threads:"));
                double score = parseDouble(get(rec, H, "score"));

                double secPerOp = Double.NaN;
                double opsPerSec = Double.NaN;

                if (mode.toLowerCase(Locale.ROOT).contains("avgt")) {
                    secPerOp = convertToSecondsPerOp(score, units);
                } else if (mode.toLowerCase(Locale.ROOT).contains("thrpt")) {
                    opsPerSec = score;
                }
                out.add(new Row(benchmark, mode.toLowerCase(Locale.ROOT), threads, secPerOp, opsPerSec));
            }
            return out;
        }
    }

    static Map<String, Integer> lowerHeader(Map<String, Integer> raw) {
        return raw.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().toLowerCase(Locale.ROOT), Map.Entry::getValue));
    }

    static String get(CSVRecord rec, Map<String, Integer> H, String... keys) {
        for (String k : keys) {
            Integer i = H.get(k.toLowerCase(Locale.ROOT));
            if (i != null && i < rec.size()) return rec.get(i);
        }
        return "";
    }

    static int parseInt(String s) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return 1; }
    }

    static double parseDouble(String s) {
        try { return Double.parseDouble(s.trim()); } catch (Exception e) { return Double.NaN; }
    }

    static double convertToSecondsPerOp(double score, String units) {
        String u = units == null ? "" : units.toLowerCase(Locale.ROOT);
        if (u.contains("s/op")) return score;
        if (u.contains("ms/op")) return score / 1_000.0;
        if (u.contains("us/op") || u.contains("µs/op")) return score / 1_000_000.0;
        if (u.contains("ns/op")) return score / 1_000_000_000.0;
        return score;
    }
}