package com.tahs.analysis;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.DecimalFormat;
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
        List<Row> latency = rows.stream().filter(r -> r.mode.contains("avgt")).toList();
        List<Row> thrpt   = rows.stream().filter(r -> r.mode.contains("thrpt")).toList();

        if (!thrpt.isEmpty()) {
            Map<Integer, Stats> thrByThreads = groupStatsByThreads(thrpt);
            plotThroughputVsThreads(thrByThreads, outDir.resolve("throughput_vs_threads.png"));
            writeSummary(thrByThreads, sumDir.resolve("summary_throughput.csv"), "threads,mean_ops_s,std_ops_s\n");
        }

        if (!latency.isEmpty()) {
            double[] valuesSec = latency.stream().mapToDouble(r -> r.scoreSecPerOp).toArray();
            plotLatencyHistogram(valuesSec, outDir.resolve("latency_histogram.png"));
            writeSummaryLatency(valuesSec, sumDir.resolve("summary_latency.csv"));
        }

        System.out.println("Plots:");
        System.out.println(" - " + outDir.resolve("throughput_vs_threads.png"));
        System.out.println(" - " + outDir.resolve("latency_histogram.png"));
        System.out.println("Summaries:");
        System.out.println(" - " + sumDir.resolve("summary_throughput.csv"));
        System.out.println(" - " + sumDir.resolve("summary_latency.csv"));
    }

    static final class Row {
        final String benchmark;
        final String mode;
        final int threads;
        final double scoreSecPerOp;   // for avgt (seconds/op)
        final double scoreOpsPerSec;  // for thrpt (ops/s)
        Row(String b, String m, int t, double sSec, double sOps) {
            this.benchmark=b; this.mode=m; this.threads=t; this.scoreSecPerOp=sSec; this.scoreOpsPerSec=sOps;
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
                    secPerOp = toSecondsPerOp(score, units);
                } else if (mode.toLowerCase(Locale.ROOT).contains("thrpt")) {
                    opsPerSec = score; // JMH thrpt is already ops/s
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

    static double toSecondsPerOp(double score, String units) {
        String u = units == null ? "" : units.toLowerCase(Locale.ROOT);
        if (u.contains("s/op"))  return score;
        if (u.contains("ms/op")) return score / 1_000.0;
        if (u.contains("us/op") || u.contains("µs/op")) return score / 1_000_000.0;
        if (u.contains("ns/op")) return score / 1_000_000_000.0;
        return score;
    }

    static final class Stats {
        final double mean;
        final double std;
        Stats(double mean, double std) { this.mean = mean; this.std = std; }
    }

    static Map<Integer, Stats> groupStatsByThreads(List<Row> thrptRows) {
        Map<Integer, List<Double>> groups = new TreeMap<>();
        for (Row r : thrptRows) {
            groups.computeIfAbsent(r.threads, k -> new ArrayList<>()).add(r.scoreOpsPerSec);
        }
        Map<Integer, Stats> out = new TreeMap<>();
        for (var e : groups.entrySet()) {
            double m = e.getValue().stream().mapToDouble(d -> d).average().orElse(Double.NaN);
            double s = stdDev(e.getValue(), m);
            out.put(e.getKey(), new Stats(m, s));
        }
        return out;
    }

    static double stdDev(List<Double> vals, double mean) {
        if (vals.size() < 2) return 0.0;
        double sum = 0;
        for (double v : vals) sum += (v - mean) * (v - mean);
        return Math.sqrt(sum / (vals.size() - 1));
    }

    static void plotThroughputVsThreads(Map<Integer, Stats> byThreads, Path outPng) throws IOException {
        int width = 900, height = 560, left = 70, right = 30, top = 40, bottom = 60;
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, width, height);
        g.setColor(Color.BLACK); g.drawString("Throughput vs Concurrency (ops/s)", left, top - 10);

        int plotW = width - left - right, plotH = height - top - bottom;
        g.drawLine(left, height - bottom, left + plotW, height - bottom);
        g.drawLine(left, height - bottom, left, top);

        List<Integer> xs = new ArrayList<>(byThreads.keySet());
        Collections.sort(xs);
        double maxY = byThreads.values().stream().mapToDouble(s -> s.mean).max().orElse(1.0);
        if (maxY <= 0) maxY = 1.0;

        g.setColor(Color.BLUE);
        int prevX = -1, prevY = -1;
        for (int t : xs) {
            double mean = byThreads.get(t).mean;
            int x = left + (int) (((double)(xs.indexOf(t)) / Math.max(1, (xs.size()-1))) * plotW);
            int y = top + (int) ((1.0 - (mean / maxY)) * plotH);
            g.fillOval(x - 3, y - 3, 6, 6);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }

        g.setColor(Color.DARK_GRAY);
        g.drawString("Threads", left + plotW / 2 - 20, height - 20);
        g.drawString("ops/s", 15, top + 15);

        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyHistogram(double[] valuesSec, Path outPng) throws IOException {
        int width = 900, height = 560, left = 70, right = 30, top = 40, bottom = 60;
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, width, height);
        g.setColor(Color.BLACK); g.drawString("Latency Distribution (seconds per book)", left, top - 10);

        int plotW = width - left - right, plotH = height - top - bottom;
        g.drawLine(left, height - bottom, left + plotW, height - bottom);
        g.drawLine(left, height - bottom, left, top);

        if (valuesSec.length == 0) {
            ImageIO.write(img, "png", outPng.toFile());
            g.dispose();
            return;
        }

        int bins = 40;
        double max = Arrays.stream(valuesSec).max().orElse(1.0);
        if (max <= 0) max = 1.0;
        int[] counts = new int[bins];
        for (double v : valuesSec) {
            int i = (int) Math.floor((v / max) * (bins - 1));
            if (i < 0) i = 0;
            if (i >= bins) i = bins - 1;
            counts[i]++;
        }
        int maxCount = Arrays.stream(counts).max().orElse(1);

        int barW = Math.max(1, plotW / bins);
        for (int i = 0; i < bins; i++) {
            int h = (int) Math.round((counts[i] / (double) maxCount) * plotH);
            int x = left + i * barW;
            int y = height - bottom - h;
            g.setColor(new Color(255, 160, 0));
            g.fillRect(x, y, Math.max(1, barW - 1), h);
        }

        g.setColor(Color.DARK_GRAY);
        g.drawString("seconds/op", left + plotW / 2 - 30, height - 20);
        g.drawString("count", 20, top + 15);

        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void writeSummary(Map<Integer, Stats> stats, Path csv, String header) throws IOException {
        DecimalFormat df = new DecimalFormat("#.####");
        StringBuilder sb = new StringBuilder(header);
        for (var e : stats.entrySet()) {
            sb.append(e.getKey()).append(',')
                    .append(df.format(e.getValue().mean)).append(',')
                    .append(df.format(e.getValue().std)).append('\n');
        }
        Files.writeString(csv, sb.toString(), StandardCharsets.UTF_8);
    }

    static void writeSummaryLatency(double[] vals, Path csv) throws IOException {
        if (vals.length == 0) {
            Files.writeString(csv, "count,mean_s,stdev_s,p50_s,p95_s,p99_s\n0,0,0,0,0,0\n", StandardCharsets.UTF_8);
            return;
        }
        Arrays.sort(vals);
        double mean = Arrays.stream(vals).average().orElse(0);
        double std = stdDev(Arrays.stream(vals).boxed().toList(), mean);
        double p50 = percentile(vals, 50);
        double p95 = percentile(vals, 95);
        double p99 = percentile(vals, 99);
        DecimalFormat df = new DecimalFormat("#.######");
        String out = "count,mean_s,stdev_s,p50_s,p95_s,p99_s\n" +
                vals.length + "," +
                df.format(mean) + "," +
                df.format(std) + "," +
                df.format(p50) + "," +
                df.format(p95) + "," +
                df.format(p99) + "\n";
        Files.writeString(csv, out, StandardCharsets.UTF_8);
    }

    static double percentile(double[] sortedAsc, double p) {
        if (sortedAsc.length == 0) return 0;
        double rank = (p / 100.0) * (sortedAsc.length - 1);
        int lo = (int) Math.floor(rank);
        int hi = (int) Math.ceil(rank);
        if (hi == lo) return sortedAsc[lo];
        double w = rank - lo;
        return sortedAsc[lo] * (1 - w) + sortedAsc[hi] * w;
    }
}
