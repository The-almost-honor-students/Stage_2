package com.tahs.analysis;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.Reader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

public class PlotIndexingBench {

    private static final String BENCH = "indexing";

    public static void main(String[] args) throws Exception {
        Path base = Path.of("benchmarking_results").resolve(BENCH);
        Path dataDir = base.resolve("data");
        Path plotsDir = base.resolve("plots");
        Files.createDirectories(plotsDir);

        Path csvPath = args.length > 0
                ? Path.of(args[0])
                : dataDir.resolve(BENCH + "_data.csv");

        List<Row> rows = load(csvPath);
        List<Row> thrpt = rows.stream().filter(r -> r.mode.equals("thrpt") && r.phase.equals("iteration")).toList();
        List<Row> avgt  = rows.stream().filter(r -> r.mode.equals("avgt")  && r.phase.equals("iteration")).toList();

        if (!thrpt.isEmpty()) {
            Map<Integer, List<Row>> thrByThreads = thrpt.stream().collect(Collectors.groupingBy(r -> r.threads, TreeMap::new, Collectors.toList()));
            Map<Integer, Stats> thrAgg = aggregateOps(thrByThreads);
            writeSummaryThrpt(thrAgg, dataDir.resolve(BENCH + "_summary_throughput.csv"));
            plotThroughputVsThreads(thrAgg, plotsDir.resolve(BENCH + "_throughput_vs_threads.png"));
            plotThroughputIterations(thrByThreads, plotsDir.resolve(BENCH + "_throughput_iterations.png"));
        }

        if (!avgt.isEmpty()) {
            Map<Integer, List<Row>> latByThreads = avgt.stream().collect(Collectors.groupingBy(r -> r.threads, TreeMap::new, Collectors.toList()));
            double[] allSec = avgt.stream().mapToDouble(r -> toSecondsPerOp(r.value, r.unit)).toArray();
            writeSummaryLatency(allSec, dataDir.resolve(BENCH + "_summary_latency.csv"));
            plotLatencyIterations(latByThreads, plotsDir.resolve(BENCH + "_latency_iterations.png"));
            plotLatencyHistogram(allSec, plotsDir.resolve(BENCH + "_latency_histogram.png"));
        }

        System.out.println("Plots in: " + plotsDir.toAbsolutePath());
    }

    static final class Row {
        final int threads;
        final String phase;
        final int iteration;
        final double value;
        final String unit;
        final String mode;
        Row(int t, String p, int i, double v, String u, String m) {
            threads = t; phase = p; iteration = i; value = v; unit = u; mode = m;
        }
    }

    static final class Stats {
        final double mean, std;
        Stats(double m, double s) { mean = m; std = s; }
    }

    static List<Row> load(Path csv) throws IOException {
        if (!Files.exists(csv)) throw new IOException("CSV not found: " + csv.toAbsolutePath());
        try (Reader r = Files.newBufferedReader(csv, StandardCharsets.UTF_8);
             CSVParser p = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim().parse(r)) {
            List<Row> out = new ArrayList<>();
            for (CSVRecord rec : p) {
                int threads = Integer.parseInt(rec.get("threads").trim());
                String phase = rec.get("phase").trim().toLowerCase(Locale.ROOT);
                int iteration = Integer.parseInt(rec.get("iteration").trim());
                double value = Double.parseDouble(rec.get("value").trim().replace(",", ""));
                String unit = rec.get("unit").trim().toLowerCase(Locale.ROOT);
                String mode = rec.get("mode").trim().toLowerCase(Locale.ROOT);
                out.add(new Row(threads, phase, iteration, value, unit, mode));
            }
            return out;
        }
    }

    static Map<Integer, Stats> aggregateOps(Map<Integer, List<Row>> thrByThreads) {
        Map<Integer, Stats> out = new TreeMap<>();
        for (var e : thrByThreads.entrySet()) {
            double[] vals = e.getValue().stream().mapToDouble(r -> r.value).toArray();
            double mean = Arrays.stream(vals).average().orElse(0);
            double std = stddev(vals, mean);
            out.put(e.getKey(), new Stats(mean, std));
        }
        return out;
    }

    static void writeSummaryThrpt(Map<Integer, Stats> thrAgg, Path csv) throws IOException {
        DecimalFormat df = new DecimalFormat("#.####");
        StringBuilder sb = new StringBuilder("threads,mean_ops_s,std_ops_s\n");
        for (var e : thrAgg.entrySet()) {
            sb.append(e.getKey()).append(',')
                    .append(df.format(e.getValue().mean)).append(',')
                    .append(df.format(e.getValue().std)).append('\n');
        }
        Files.writeString(csv, sb.toString(), StandardCharsets.UTF_8);
    }

    static void writeSummaryLatency(double[] sec, Path csv) throws IOException {
        Arrays.sort(sec);
        double mean = Arrays.stream(sec).average().orElse(0);
        double std = stddev(sec, mean);
        double p50 = percentile(sec, 50), p95 = percentile(sec, 95), p99 = percentile(sec, 99);
        DecimalFormat df = new DecimalFormat("#.######");
        String out = "count,mean_s,stdev_s,p50_s,p95_s,p99_s\n" +
                sec.length + "," + df.format(mean) + "," + df.format(std) + "," +
                df.format(p50) + "," + df.format(p95) + "," + df.format(p99) + "\n";
        Files.writeString(csv, out, StandardCharsets.UTF_8);
    }

    // --- Plots ---

    static void plotThroughputVsThreads(Map<Integer, Stats> agg, Path outPng) throws IOException {
        int w = 960, h = 560, l = 70, r = 30, t = 50, b = 60;
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);
        g.setColor(Color.BLACK); g.drawString("Throughput vs Threads (ops/s)", l, t - 15);
        int plotW = w - l - r, plotH = h - t - b;
        g.drawLine(l, h - b, l + plotW, h - b);
        g.drawLine(l, h - b, l, t);

        List<Integer> xs = new ArrayList<>(agg.keySet());
        Collections.sort(xs);
        double maxY = agg.values().stream().mapToDouble(s -> s.mean).max().orElse(1.0);
        if (maxY <= 0) maxY = 1.0;

        g.setColor(Color.BLUE);
        int prevX = -1, prevY = -1;
        for (int i = 0; i < xs.size(); i++) {
            int thr = xs.get(i);
            double mean = agg.get(thr).mean;
            int x = l + (int) (i * (plotW / Math.max(1.0, xs.size() - 1.0)));
            int y = t + (int) ((1.0 - mean / maxY) * plotH);
            g.fillOval(x - 3, y - 3, 6, 6);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }
        g.setColor(Color.DARK_GRAY);
        g.drawString("Threads", l + plotW / 2 - 20, h - 20);
        g.drawString("ops/s", 15, t + 15);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotThroughputIterations(Map<Integer, List<Row>> byThreads, Path outPng) throws IOException {
        int w = 1100, h = 620, l = 70, r = 30, t = 50, b = 60;
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);
        g.setColor(Color.BLACK); g.drawString("Throughput per Iteration (ops/s)", l, t - 15);
        int plotW = w - l - r, plotH = h - t - b;
        g.drawLine(l, h - b, l + plotW, h - b);
        g.drawLine(l, h - b, l, t);

        int maxIter = byThreads.values().stream().mapToInt(list -> list.stream().mapToInt(row -> row.iteration).max().orElse(1)).max().orElse(1);
        double maxY = byThreads.values().stream().flatMap(List::stream).mapToDouble(row -> row.value).max().orElse(1.0);
        if (maxY <= 0) maxY = 1.0;

        Color[] colors = palette();
        int ci = 0;

        for (var e : byThreads.entrySet()) {
            int thr = e.getKey();
            List<Row> list = e.getValue().stream().sorted(Comparator.comparingInt(row -> row.iteration)).toList();
            g.setColor(colors[ci++ % colors.length]);
            int prevX = -1, prevY = -1;
            for (Row rrow : list) {
                int x = l + (int) ((rrow.iteration - 1) * (plotW / Math.max(1.0, maxIter - 1.0)));
                int y = t + (int) ((1.0 - rrow.value / maxY) * plotH);
                g.fillOval(x - 2, y - 2, 4, 4);
                if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
                prevX = x; prevY = y;
            }
            g.drawString(thr + "t", l + 10 + (ci * 50) % (plotW - 50), t + 15 + 15 * ((ci * 50) / (plotW - 50)));
        }
        g.setColor(Color.DARK_GRAY);
        g.drawString("Iteration", l + plotW / 2 - 25, h - 20);
        g.drawString("ops/s", 15, t + 15);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyIterations(Map<Integer, List<Row>> byThreads, Path outPng) throws IOException {
        int w = 1100, h = 620, l = 70, r = 30, t = 50, b = 60;
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);
        g.setColor(Color.BLACK); g.drawString("Latency per Iteration (seconds/op)", l, t - 15);
        int plotW = w - l - r, plotH = h - t - b;
        g.drawLine(l, h - b, l + plotW, h - b);
        g.drawLine(l, h - b, l, t);

        int maxIter = byThreads.values().stream().mapToInt(list -> list.stream().mapToInt(row -> row.iteration).max().orElse(1)).max().orElse(1);
        double maxY = byThreads.values().stream()
                .flatMap(List::stream)
                .mapToDouble(row -> toSecondsPerOp(row.value, row.unit))
                .max().orElse(1.0);
        if (maxY <= 0) maxY = 1.0;

        Color[] colors = palette();
        int ci = 0;

        for (var e : byThreads.entrySet()) {
            int thr = e.getKey();
            List<Row> list = e.getValue().stream().sorted(Comparator.comparingInt(row -> row.iteration)).toList();
            g.setColor(colors[ci++ % colors.length]);
            int prevX = -1, prevY = -1;
            for (Row rrow : list) {
                double sec = toSecondsPerOp(rrow.value, rrow.unit);
                int x = l + (int) ((rrow.iteration - 1) * (plotW / Math.max(1.0, maxIter - 1.0)));
                int y = t + (int) ((1.0 - sec / maxY) * plotH);
                g.fillOval(x - 2, y - 2, 4, 4);
                if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
                prevX = x; prevY = y;
            }
            g.drawString(thr + "t", l + 10 + (ci * 50) % (plotW - 50), t + 15 + 15 * ((ci * 50) / (plotW - 50)));
        }
        g.setColor(Color.DARK_GRAY);
        g.drawString("Iteration", l + plotW / 2 - 25, h - 20);
        g.drawString("seconds/op", 15, t + 15);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyHistogram(double[] sec, Path outPng) throws IOException {
        int w = 960, h = 560, l = 70, r = 30, t = 50, b = 60;
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);
        g.setColor(Color.BLACK); g.drawString("Latency Distribution (seconds/op)", l, t - 15);
        int plotW = w - l - r, plotH = h - t - b;
        g.drawLine(l, h - b, l + plotW, h - b);
        g.drawLine(l, h - b, l, t);

        if (sec.length == 0) {
            ImageIO.write(img, "png", outPng.toFile()); g.dispose(); return;
        }

        int bins = 40;
        double max = Arrays.stream(sec).max().orElse(1.0);
        if (max <= 0) max = 1.0;

        int[] counts = new int[bins];
        for (double v : sec) {
            int i = (int) Math.floor((v / max) * (bins - 1));
            if (i < 0) i = 0; if (i >= bins) i = bins - 1;
            counts[i]++;
        }
        int maxCount = Arrays.stream(counts).max().orElse(1);

        int barW = Math.max(1, plotW / bins);
        for (int i = 0; i < bins; i++) {
            int hBar = (int) Math.round((counts[i] / (double) maxCount) * plotH);
            int x = l + i * barW;
            int y = h - b - hBar;
            g.setColor(new Color(255, 160, 0));
            g.fillRect(x, y, Math.max(1, barW - 1), hBar);
        }
        g.setColor(Color.DARK_GRAY);
        g.drawString("seconds/op", l + plotW / 2 - 30, h - 20);
        g.drawString("count", 20, t + 15);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static double toSecondsPerOp(double value, String unit) {
        String u = unit == null ? "" : unit.toLowerCase(Locale.ROOT);
        if (u.contains("s/op"))  return value;
        if (u.contains("ms/op")) return value / 1_000.0;
        if (u.contains("us/op") || u.contains("µs/op")) return value / 1_000_000.0;
        if (u.contains("ns/op")) return value / 1_000_000_000.0;
        return value;
    }

    static double percentile(double[] sortedAsc, double p) {
        if (sortedAsc.length == 0) return 0;
        double rank = (p / 100.0) * (sortedAsc.length - 1);
        int lo = (int) Math.floor(rank), hi = (int) Math.ceil(rank);
        if (hi == lo) return sortedAsc[lo];
        double w = rank - lo;
        return sortedAsc[lo] * (1 - w) + sortedAsc[hi] * w;
    }

    static double stddev(double[] vals, double mean) {
        if (vals.length < 2) return 0;
        double s = 0; for (double v : vals) s += (v - mean) * (v - mean);
        return Math.sqrt(s / (vals.length - 1));
    }

    static Color[] palette() {
        return new Color[]{
                new Color(30,144,255), new Color(46,204,113), new Color(155,89,182),
                new Color(241,196,15), new Color(231,76,60),  new Color(52,73,94),
                new Color(26,188,156), new Color(230,126,34)
        };
    }
}
