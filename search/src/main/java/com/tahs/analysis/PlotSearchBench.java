package com.tahs.analysis;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageOutputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.DecimalFormat;
import java.util.List;
import java.util.*;
import java.util.stream.Collectors;

public class PlotSearchBench {

    private static final String BENCH = "search";
    private static final Path BASE = Path.of("benchmarking_results").resolve(BENCH);
    private static final Path DATA = BASE.resolve("data");
    private static final Path PLOTS = BASE.resolve("plots");

    public static void main(String[] args) throws Exception {
        Files.createDirectories(DATA);
        Files.createDirectories(PLOTS);

        Path samplesCsv = (args.length > 0) ? Path.of(args[0]) : DATA.resolve("search_samples.csv");
        if (!Files.exists(samplesCsv)) {
            Path fallback = BASE.resolve("search_samples.csv");
            if (Files.exists(fallback)) samplesCsv = fallback;
        }

        List<Row> rows = loadSamples(samplesCsv);
        if (rows.isEmpty()) return;

        writeOverallSummary(rows, DATA.resolve("search_summary_overall.csv"));
        writeByTermSummary(rows, DATA.resolve("search_summary_by_term.csv"));

        plotLatencyHistogram(rows, PLOTS.resolve("search_latency_histogram.png"));
        plotLatencyCDF(rows, PLOTS.resolve("search_latency_cdf.png"));
        plotLatencyOverTime(rows, PLOTS.resolve("search_latency_over_time.png"));
        plotLatencyByTermP50P95(rows, PLOTS.resolve("search_latency_p50_p95_by_term.png"));
        plotCpuOverTime(rows, PLOTS.resolve("search_cpu_over_time.png"));
        plotMemoryOverTime(rows, PLOTS.resolve("search_memory_over_time.png"));
        plotLatencyVsCpu(rows, PLOTS.resolve("search_latency_vs_cpu.png"));
        plotLatencyVsMemory(rows, PLOTS.resolve("search_latency_vs_memory.png"));
        plotLatencyBoxplotByTerm(rows, PLOTS.resolve("search_latency_boxplot_by_term.png"));
        plotLatencyMeanByTerm(rows, PLOTS.resolve("search_latency_mean_by_term.png"));
        plotLatencyPercentiles(rows, PLOTS.resolve("search_latency_percentiles.png"));

        Path thrCsv = DATA.resolve("search_throughput_summary.csv");
        if (Files.exists(thrCsv)) {
            plotThroughputVsThreads(thrCsv, PLOTS.resolve("search_throughput_vs_threads.png"));
            writeThroughputEfficiency(thrCsv, DATA.resolve("search_throughput_efficiency.csv"));
            plotSpeedupEfficiency(DATA.resolve("search_throughput_efficiency.csv"), PLOTS.resolve("search_speedup_efficiency.png"));
        }
    }

    /* ===================== Data Model ===================== */

    static final class Row {
        final long tsMs;
        final String term;
        final double latencyMs;
        final double cpuPct;
        final double usedMb;
        final double totalMb;

        Row(long tsMs, String term, double latencyMs, double cpuPct, double usedMb, double totalMb) {
            this.tsMs = tsMs;
            this.term = term;
            this.latencyMs = latencyMs;
            this.cpuPct = cpuPct;
            this.usedMb = usedMb;
            this.totalMb = totalMb;
        }
    }

    static List<Row> loadSamples(Path csv) throws IOException {
        if (!Files.exists(csv)) return List.of();
        try (Reader r = Files.newBufferedReader(csv, StandardCharsets.UTF_8);
             CSVParser p = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim().parse(r)) {
            List<Row> out = new ArrayList<>();
            for (CSVRecord rec : p) {
                try {
                    long ts = Long.parseLong(rec.get("timestamp_ms").trim());
                    String term = rec.get("term").trim();
                    double lat = Double.parseDouble(rec.get("latency_ms").trim());
                    double cpu = parseDoubleSafe(rec, "cpu_percent");
                    double used = parseDoubleSafe(rec, "used_memory_mb");
                    double tot = parseDoubleSafe(rec, "total_memory_mb");
                    out.add(new Row(ts, term, lat, cpu, used, tot));
                } catch (Exception ignored) {}
            }
            return out;
        }
    }

    static double parseDoubleSafe(CSVRecord rec, String key) {
        try { return Double.parseDouble(rec.get(key).trim()); }
        catch (Exception e) { return Double.NaN; }
    }

    static double[] latencies(List<Row> rows) {
        double[] v = rows.stream().mapToDouble(r -> r.latencyMs).toArray();
        Arrays.sort(v);
        return v;
    }

    static double percentile(double[] sortedAsc, double p) {
        if (sortedAsc.length == 0) return Double.NaN;
        double rank = (p / 100.0) * (sortedAsc.length - 1);
        int lo = (int) Math.floor(rank), hi = (int) Math.ceil(rank);
        if (hi == lo) return sortedAsc[lo];
        double w = rank - lo;
        return sortedAsc[lo] * (1 - w) + sortedAsc[hi] * w;
    }

    static double mean(double[] arr) {
        if (arr.length == 0) return Double.NaN;
        double s = 0; for (double v : arr) s += v; return s / arr.length;
    }


    static void aa(Graphics2D g) {
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
    }

    static Color brandBlue() { return new Color(0, 102, 204); }

    static double[] niceTicks(double min, double max, int maxTicks) {
        if (Double.isNaN(min) || Double.isNaN(max) || min == max) return new double[]{min, max, 1};
        double range = niceNum(max - min, false);
        double d = niceNum(range / Math.max(2, (maxTicks - 1)), true);
        double niceMin = Math.floor(min / d) * d;
        double niceMax = Math.ceil(max / d) * d;
        return new double[]{niceMin, niceMax, d};
    }

    static double niceNum(double x, boolean round) {
        double exp = Math.floor(Math.log10(x));
        double f = x / Math.pow(10, exp);
        double nf = round ? ((f < 1.5) ? 1 : (f < 3) ? 2 : (f < 7) ? 5 : 10)
                : ((f <= 1) ? 1 : (f <= 2) ? 2 : (f <= 5) ? 5 : 10);
        return nf * Math.pow(10, exp);
    }

    static void drawYLinear(Graphics2D g, int l, int t, int plotW, int plotH,
                            double minY, double maxY, int ticks, String unitSuffix) {
        g.setColor(Color.BLACK);
        g.drawLine(l, t - 6, l, t + plotH + 6);
        g.drawLine(l, t + plotH, l + plotW + 6, t + plotH);

        double[] nt = niceTicks(minY, maxY, Math.max(3, ticks));
        double y0 = nt[0], y1 = nt[1], step = nt[2];

        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();
        for (double v = y0; v <= y1 + 1e-12; v += step) {
            int y = t + (int) Math.round((1.0 - (v - y0) / (y1 - y0)) * plotH);
            g.setColor(new Color(230, 230, 230));
            g.drawLine(l, y, l + plotW, y);

            String label = (unitSuffix == null)
                    ? String.format(Locale.ROOT, "%.2f", v)
                    : String.format(Locale.ROOT, "%.2f%s", v, unitSuffix);

            int tw = fm.stringWidth(label);
            int ty = y + fm.getAscent() / 2 - 2;

            g.setColor(Color.WHITE);
            g.fillRoundRect(l - 12 - tw, ty - fm.getAscent(), tw + 8, fm.getHeight(), 6, 6);

            g.setColor(Color.BLACK);
            g.drawString(label, l - 8 - tw, ty);
            g.drawLine(l - 4, y, l, y);
        }
    }

    static void drawLabel(Graphics2D g, String text, int x, int y) {
        FontMetrics fm = g.getFontMetrics();
        int tw = fm.stringWidth(text);
        g.setColor(Color.WHITE);
        g.fillRoundRect(x - 4, y - fm.getAscent(), tw + 8, fm.getHeight(), 6, 6);
        g.setColor(Color.BLACK);
        g.drawString(text, x, y);
    }

    static void drawXAxisTitle(Graphics2D g, String text, int l, int t, int plotW, int plotH) {
        g.setFont(g.getFont().deriveFont(Font.BOLD, 16f));
        FontMetrics fm = g.getFontMetrics();
        int x = l + (plotW - fm.stringWidth(text)) / 2;
        int y = t + plotH + 48;
        g.setColor(Color.DARK_GRAY);
        g.drawString(text, x, y);
    }

    static void drawLegend(Graphics2D g, List<String> labels, List<Color> colors, int x, int y) {
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 14f));
        int dx = 0;
        for (int i = 0; i < labels.size(); i++) {
            Color c = colors.get(i);
            g.setColor(c);
            g.fillRect(x + dx, y - 10, 18, 8);
            g.setColor(Color.BLACK);
            g.drawRect(x + dx, y - 10, 18, 8);
            g.drawString(labels.get(i), x + dx + 24, y);
            dx += 24 + g.getFontMetrics().stringWidth(labels.get(i)) + 24;
        }
    }

    static void drawXTimeTicks(Graphics2D g, int l, int t, int plotW, int plotH, long minTs, long maxTs) {
        double minS = 0;
        double maxS = Math.max(1e-9, (maxTs - minTs) / 1000.0);
        double[] nt = niceTicks(minS, maxS, 6);

        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();
        for (double v = nt[0]; v <= nt[1] + 1e-12; v += nt[2]) {
            int x = l + (int) Math.round(((v - nt[0]) / (nt[1] - nt[0])) * plotW);
            g.setColor(new Color(220, 220, 220));
            g.drawLine(x, t, x, t + plotH);

            String lbl = String.format(Locale.ROOT, "%.0f s", v);
            int tw = fm.stringWidth(lbl);
            g.setColor(Color.BLACK);
            g.drawString(lbl, x - tw / 2, t + plotH + fm.getAscent() * 2 + 6);
        }
    }

    static void drawXTicksTerms(Graphics2D g, int left, int top, int plotW, int plotH, List<String> terms) {
        int n = terms.size();
        int groupW = (int) Math.floor(plotW / (double) n);
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();

        for (int i = 0; i < n; i++) {
            int x = left + i * groupW + groupW / 2;
            String term = terms.get(i);
            int tw = fm.stringWidth(term);

            g.setColor(Color.BLACK);
            g.drawString(term, x - tw / 2, top + plotH + fm.getAscent() + 20);

            g.setColor(new Color(220, 220, 220));
            g.drawLine(x, top, x, top + plotH);
        }
    }

    /* ===================== CSV Summaries ===================== */

    static void writeOverallSummary(List<Row> rows, Path csv) throws IOException {
        double[] lat = latencies(rows);
        DecimalFormat df = new DecimalFormat("#.###");
        String out = "count,avg_ms,p50_ms,p95_ms\n" +
                lat.length + "," + df.format(mean(lat)) + "," +
                df.format(percentile(lat, 50)) + "," +
                df.format(percentile(lat, 95)) + "\n";
        Files.writeString(csv, out, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    static void writeByTermSummary(List<Row> rows, Path csv) throws IOException {
        Map<String, List<Row>> byTerm = rows.stream().collect(Collectors.groupingBy(r -> r.term));
        DecimalFormat df = new DecimalFormat("#.###");
        StringBuilder sb = new StringBuilder("term,count,avg_ms,p50_ms,p95_ms\n");
        for (var e : byTerm.entrySet()) {
            double[] lat = latencies(e.getValue());
            sb.append(e.getKey()).append(',').append(lat.length).append(',')
                    .append(df.format(mean(lat))).append(',')
                    .append(df.format(percentile(lat, 50))).append(',')
                    .append(df.format(percentile(lat, 95))).append('\n');
        }
        Files.writeString(sb.length() > 0 ? csv : csv, sb.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    static void plotLatencyHistogram(List<Row> rows, Path outPng) throws IOException {
        int w = 1100, h = 700, l = 110, r = 50, t = 100, b = 100;
        int plotW = w - l - r, plotH = h - t - b;

        double[] lat = latencies(rows);
        if (lat.length == 0) return;

        int bins = Math.min(50, Math.max(20, lat.length / 100));
        double max = lat[lat.length - 1]; if (max <= 0) max = 1;

        int[] counts = new int[bins];
        for (double v : lat) {
            int i = (int) Math.floor((v / max) * (bins - 1));
            if (i < 0) i = 0; if (i >= bins) i = bins - 1;
            counts[i]++;
        }
        int maxC = Arrays.stream(counts).max().orElse(1);

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);

        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Search — Latency Histogram (ms)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title)) / 2, 50);

        drawYLinear(g, l, t, plotW, plotH, 0, maxC, 6, "");
        double[] ntX = niceTicks(0, max, 6);

        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();
        for (double v = ntX[0]; v <= ntX[1] + 1e-12; v += ntX[2]) {
            int x = l + (int) Math.round(((v - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
            g.setColor(new Color(220, 220, 220)); g.drawLine(x, t, x, t + plotH);
            String lbl = String.format(Locale.ROOT, "%.0f ms", v);
            int tw = fm.stringWidth(lbl); g.setColor(Color.BLACK);
            g.drawString(lbl, x - tw / 2, t + plotH + fm.getAscent() * 2 + 6);
        }

        int barW = Math.max(1, plotW / bins);
        g.setColor(brandBlue());
        for (int i = 0; i < bins; i++) {
            int hBar = (int) Math.round((counts[i] / (double) maxC) * plotH);
            int x = l + i * barW;
            int y = t + plotH - hBar;
            g.fillRect(x, y, Math.max(1, barW - 2), hBar);
        }

        double p50 = percentile(lat, 50), p95 = percentile(lat, 95);
        int x50 = l + (int) Math.round(((p50 - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
        int x95 = l + (int) Math.round(((p95 - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
        g.setStroke(new BasicStroke(2f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1f, new float[]{6f, 6f}, 0f));
        g.setColor(new Color(0, 0, 0, 160));
        g.drawLine(x50, t, x50, t + plotH);
        g.drawLine(x95, t, x95, t + plotH);

        g.setFont(g.getFont().deriveFont(Font.BOLD, 14f));
        g.setColor(Color.BLACK);
        g.drawString("P50", x50 + 8, t + 20);
        g.drawString("P95", x95 + 8, t + 38);

        drawXAxisTitle(g, "Latency (ms)", l, t, plotW, plotH);
        drawLegend(g, List.of("P50", "P95"),
                List.of(new Color(0, 0, 0, 160), new Color(0, 0, 0, 160)),
                l + 20, t - 20);

        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyCDF(List<Row> rows, Path outPng) throws IOException {
        int w = 1100, h = 700, l = 110, r = 50, t = 100, b = 100;
        int plotW = w - l - r, plotH = h - t - b;

        double[] lat = latencies(rows);
        if (lat.length == 0) return;

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);

        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Search — Latency CDF (ms)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title)) / 2, 50);

        double maxX = lat[lat.length - 1];
        double[] ntX = niceTicks(0, maxX, 6);
        drawYLinear(g, l, t, plotW, plotH, 0, 1.0, 6, "");

        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();
        for (double v = ntX[0]; v <= ntX[1] + 1e-12; v += ntX[2]) {
            int x = l + (int) Math.round(((v - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
            g.setColor(new Color(220, 220, 220)); g.drawLine(x, t, x, t + plotH);
            String lbl = String.format(Locale.ROOT, "%.0f ms", v);
            int tw = fm.stringWidth(lbl); g.setColor(Color.BLACK);
            g.drawString(lbl, x - tw / 2, t + plotH + fm.getAscent() * 2 + 6);
        }

        g.setColor(brandBlue());
        g.setStroke(new BasicStroke(2.2f));
        int prevX = -1, prevY = -1;
        for (int i = 0; i < lat.length; i++) {
            double xVal = lat[i];
            double yVal = (lat.length == 1) ? 1.0 : (i / (double) (lat.length - 1));
            int x = l + (int) Math.round(((xVal - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
            int y = t + (int) Math.round((1.0 - yVal) * plotH);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }

        double p50 = percentile(lat, 50), p95 = percentile(lat, 95);
        int x50 = l + (int) Math.round(((p50 - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
        int x95 = l + (int) Math.round(((p95 - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
        g.setStroke(new BasicStroke(2f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 1f, new float[]{6, 6}, 0));
        g.setColor(new Color(0, 0, 0, 160));
        g.drawLine(x50, t, x50, t + plotH);
        g.drawLine(x95, t, x95, t + plotH);

        g.setFont(g.getFont().deriveFont(Font.BOLD, 14f));
        g.setColor(Color.BLACK);
        g.drawString("P50", x50 + 8, t + 22);
        g.drawString("P95", x95 + 8, t + 40);

        drawXAxisTitle(g, "Latency (ms)", l, t, plotW, plotH);
        drawLegend(g, List.of("CDF", "P50", "P95"),
                List.of(brandBlue(), new Color(0, 0, 0, 160), new Color(0, 0, 0, 160)),
                l + 20, t - 20);

        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyOverTime(List<Row> rows, Path outPng) throws IOException {
        int w = 1200, h = 720, l = 120, r = 60, t = 100, b = 120;
        int plotW = w - l - r, plotH = h - t - b;

        List<Row> sorted = rows.stream().sorted(Comparator.comparingLong(rw -> rw.tsMs)).toList();
        long minTs = sorted.get(0).tsMs;
        long maxTs = sorted.get(sorted.size() - 1).tsMs; if (maxTs == minTs) maxTs = minTs + 1;

        double maxY = sorted.stream().mapToDouble(ro -> ro.latencyMs).max().orElse(1.0);
        double[] ntY = niceTicks(0, maxY * 1.05, 6);

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);

        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Search — Latency Over Time (ms)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title)) / 2, 50);

        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " ms");
        drawXTimeTicks(g, l, t, plotW, plotH, minTs, maxTs);

        g.setColor(brandBlue());
        g.setStroke(new BasicStroke(2.0f));
        double y0 = ntY[0], y1 = ntY[1];
        int prevX = -1, prevY = -1;
        for (Row rw : sorted) {
            double xS = (rw.tsMs - minTs) / 1000.0;
            double minS = 0, maxS = (maxTs - minTs) / 1000.0;
            int x = l + (int) Math.round(((xS - minS) / Math.max(1e-9, (maxS - minS))) * plotW);
            int y = t + (int) Math.round((1.0 - (rw.latencyMs - y0) / (y1 - y0)) * plotH);
            g.fillOval(x - 2, y - 2, 4, 4);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }

        drawXAxisTitle(g, "Time (s)", l, t, plotW, plotH);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyByTermP50P95(List<Row> rows, Path outPng) throws IOException {
        int w = 1200, h = 720, l = 120, r = 60, t = 100, b = 140;
        int plotW = w - l - r, plotH = h - t - b;

        Map<String, double[]> byTerm = new TreeMap<>();
        Map<String, List<Row>> groups = rows.stream()
                .collect(Collectors.groupingBy(ro -> ro.term, TreeMap::new, Collectors.toList()));

        double maxStat = 0;
        for (var e : groups.entrySet()) {
            double[] lat = latencies(e.getValue());
            double p50 = percentile(lat, 50), p95 = percentile(lat, 95);
            byTerm.put(e.getKey(), new double[]{p50, p95});
            maxStat = Math.max(maxStat, Math.max(p50, p95));
        }

        List<String> terms = new ArrayList<>(byTerm.keySet());
        if (terms.isEmpty()) return;

        double[] ntY = niceTicks(0, maxStat * 1.15, 6);

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);

        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Search — P50 / P95 Latency by Term (ms)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title)) / 2, 50);

        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " ms");

        int n = terms.size();
        int groupW = (int) Math.floor(plotW / (double) n);
        int barW   = Math.max(12, (int) Math.floor(groupW * 0.32));
        int gap    = Math.max(8, (groupW - 2 * barW) / 3);

        drawXTicksTerms(g, l, t, plotW, plotH, terms);

        Color p50Color = brandBlue();
        Color p95Color = new Color(30, 144, 255);
        FontMetrics fm = g.getFontMetrics();
        double y0 = ntY[0], y1 = ntY[1];

        for (int i = 0; i < n; i++) {
            int gx = l + i * groupW;
            double[] stats = byTerm.get(terms.get(i));
            double p50 = stats[0], p95 = stats[1];

            int xP50 = gx + gap;
            int xP95 = xP50 + barW + gap;

            int hP50 = (int) Math.round(((p50 - y0) / (y1 - y0)) * plotH);
            int hP95 = (int) Math.round(((p95 - y0) / (y1 - y0)) * plotH);

            int yP50 = t + plotH - hP50;
            int yP95 = t + plotH - hP95;

            g.setColor(p50Color); g.fillRect(xP50, yP50, barW, hP50);
            g.setColor(Color.BLACK); g.drawRect(xP50, yP50, barW, hP50);

            g.setColor(p95Color); g.fillRect(xP95, yP95, barW, hP95);
            g.setColor(Color.BLACK); g.drawRect(xP95, yP95, barW, hP95);

            String l50 = Math.round(p50) + " ms", l95 = Math.round(p95) + " ms";
            int tw50 = fm.stringWidth(l50), tw95 = fm.stringWidth(l95);
            drawLabel(g, l50, xP50 + barW / 2 - tw50 / 2, Math.max(t + 18, yP50 - 8));
            drawLabel(g, l95, xP95 + barW / 2 - tw95 / 2, Math.max(t + 18, yP95 - 8));
        }

        drawLegend(g, List.of("P50", "P95"), List.of(p50Color, p95Color), l + 20, t - 20);
        drawXAxisTitle(g, "Term", l, t, plotW, plotH);

        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyBoxplotByTerm(List<Row> rows, Path out) throws IOException {
        Map<String, List<Double>> map = rows.stream().collect(Collectors.groupingBy(r -> r.term, TreeMap::new, Collectors.mapping(r -> r.latencyMs, Collectors.toList())));
        int w = 1100, h = 700, l = 120, r = 50, t = 100, b = 150, plotW = w - l - r, plotH = h - t - b;
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        aa(g);
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, w, h);
        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        g.drawString("Latency Boxplot by Term", (w - 400) / 2, 50);
        List<String> terms = new ArrayList<>(map.keySet());
        double maxLat = rows.stream().mapToDouble(ro -> ro.latencyMs).max().orElse(1);
        double[] ntY = niceTicks(0, maxLat, 6);
        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " ms");
        drawXTicksTerms(g, l, t, plotW, plotH, terms);
        int n = terms.size(), groupW = (int) Math.floor(plotW / (double) n), boxW = Math.max(20, (int) Math.floor(groupW * 0.4));
        for (int i = 0; i < n; i++) {
            List<Double> vals = map.get(terms.get(i));
            Collections.sort(vals);
            double[] arr = vals.stream().mapToDouble(Double::doubleValue).toArray();
            double q1 = percentile(arr, 25);
            double q2 = percentile(arr, 50);
            double q3 = percentile(arr, 75);
            int gx = l + i * groupW;
            int x = gx + groupW / 2 - boxW / 2;
            int yTop = t + (int) Math.round((1.0 - (q3 - ntY[0]) / (ntY[1] - ntY[0])) * plotH);
            int yBot = t + (int) Math.round((1.0 - (q1 - ntY[0]) / (ntY[1] - ntY[0])) * plotH);
            int yMed = t + (int) Math.round((1.0 - (q2 - ntY[0]) / (ntY[1] - ntY[0])) * plotH);
            g.setColor(brandBlue());
            g.fillRect(x, yTop, boxW, Math.max(1, yBot - yTop));
            g.setColor(Color.BLACK);
            g.drawRect(x, yTop, boxW, Math.max(1, yBot - yTop));
            g.drawLine(x, yMed, x + boxW, yMed);
        }
        drawXAxisTitle(g, "Term", l, t, plotW, plotH);
        ImageIO.write(img, "png", out.toFile());
        g.dispose();
    }

    static void plotLatencyMeanByTerm(List<Row> rows, Path out) throws IOException {
        Map<String, Double> means = rows.stream()
                .collect(Collectors.groupingBy(r -> r.term, TreeMap::new, Collectors.averagingDouble(r -> r.latencyMs)));
        if (means.isEmpty()) return;
        int w = 1100, h = 700, l = 120, r = 50, t = 100, b = 150;
        int plotW = w - l - r, plotH = h - t - b;
        List<String> terms = new ArrayList<>(means.keySet());
        double maxLat = means.values().stream().mapToDouble(Double::doubleValue).max().orElse(1.0);
        double[] ntY = niceTicks(0, maxLat * 1.10, 6);
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        aa(g);
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, w, h);
        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Average Latency by Term";
        g.drawString(title, (w - g.getFontMetrics().stringWidth(title)) / 2, 50);
        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " ms");
        drawXTicksTerms(g, l, t, plotW, plotH, terms);
        int n = terms.size();
        int groupW = (int) Math.floor(plotW / (double) n);
        int barW = Math.max(24, (int) Math.floor(groupW * 0.45));
        double y0 = ntY[0], y1 = ntY[1];
        for (int i = 0; i < n; i++) {
            String term = terms.get(i);
            double val = means.get(term);
            int gx = l + i * groupW;
            int x = gx + (groupW - barW) / 2;
            int barH = (int) Math.round(((val - y0) / (y1 - y0)) * plotH);
            int y = t + plotH - barH;
            g.setColor(brandBlue());
            g.fillRect(x, y, barW, Math.max(1, barH));
            g.setColor(Color.BLACK);
            g.drawRect(x, y, barW, Math.max(1, barH));
        }
        drawXAxisTitle(g, "Term", l, t, plotW, plotH);
        ImageIO.write(img, "png", out.toFile());
        g.dispose();
    }

    static void plotLatencyPercentiles(List<Row> rows, Path out) throws IOException {
        double[] lat = rows.stream().mapToDouble(r -> r.latencyMs).sorted().toArray();
        if (lat.length == 0) return;

        double p50 = percentile(lat, 50);
        double p90 = percentile(lat, 90);
        double p95 = percentile(lat, 95);
        double p99 = percentile(lat, 99);

        Map<String, Double> map = new LinkedHashMap<>();
        map.put("P50", p50);
        map.put("P90", p90);
        map.put("P95", p95);
        map.put("P99", p99);

        int w = 1000, h = 700, l = 120, r = 50, t = 100, b = 150;
        int plotW = w - l - r, plotH = h - t - b;
        double maxY = map.values().stream().mapToDouble(Double::doubleValue).max().orElse(1.0);
        double[] ntY = niceTicks(0, maxY * 1.1, 6);

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        aa(g);
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, w, h);
        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Latency Percentiles";
        g.drawString(title, (w - g.getFontMetrics().stringWidth(title)) / 2, 50);

        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " ms");
        List<String> labels = new ArrayList<>(map.keySet());
        drawXTicksTerms(g, l, t, plotW, plotH, labels);

        int n = labels.size();
        int groupW = (int) Math.floor(plotW / (double) n);
        int barW = Math.max(24, (int) Math.floor(groupW * 0.45));
        double y0 = ntY[0], y1 = ntY[1];

        for (int i = 0; i < n; i++) {
            String label = labels.get(i);
            double val = map.get(label);
            int gx = l + i * groupW;
            int x = gx + (groupW - barW) / 2;
            int barH = (int) Math.round(((val - y0) / (y1 - y0)) * plotH);
            int y = t + plotH - barH;
            g.setColor(brandBlue());
            g.fillRect(x, y, barW, Math.max(1, barH));
            g.setColor(Color.BLACK);
            g.drawRect(x, y, barW, Math.max(1, barH));
            String txt = String.format("%.0f ms", val);
            g.drawString(txt, x + barW / 2 - g.getFontMetrics().stringWidth(txt) / 2, y - 6);
        }

        drawXAxisTitle(g, "Percentile", l, t, plotW, plotH);
        ImageIO.write(img, "png", out.toFile());
        g.dispose();
    }

    static void plotCpuOverTime(List<Row> rows, Path outPng) throws IOException {
        int w = 1100, h = 700, l = 110, r = 50, t = 100, b = 100;
        int plotW = w - l - r, plotH = h - t - b;

        List<Row> s = rows.stream().sorted(Comparator.comparingLong(rw -> rw.tsMs)).toList();
        long minTs = s.get(0).tsMs, maxTs = s.get(s.size() - 1).tsMs;

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);

        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "CPU Usage Over Time (%)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title)) / 2, 50);

        drawYLinear(g, l, t, plotW, plotH, 0, 100, 6, " %");
        drawXTimeTicks(g, l, t, plotW, plotH, minTs, maxTs);

        g.setColor(Color.BLACK);
        int prevX = -1, prevY = -1;
        for (Row rw : s) {
            double xS = (rw.tsMs - minTs) / 1000.0;
            double maxS = (maxTs - minTs) / 1000.0;
            int x = l + (int) Math.round((xS / Math.max(1e-9, maxS)) * plotW);
            double cpu = Math.max(0, Math.min(100, Double.isNaN(rw.cpuPct) ? 0 : rw.cpuPct));
            int y = t + (int) Math.round((1.0 - cpu / 100.0) * plotH);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            g.fillOval(x - 2, y - 2, 4, 4);
            prevX = x; prevY = y;
        }

        drawXAxisTitle(g, "Time (s)", l, t, plotW, plotH);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotMemoryOverTime(List<Row> rows, Path outPng) throws IOException {
        int w = 1100, h = 700, l = 110, r = 50, t = 100, b = 100;
        int plotW = w - l - r, plotH = h - t - b;

        List<Row> s = rows.stream().sorted(Comparator.comparingLong(rw -> rw.tsMs)).toList();
        long minTs = s.get(0).tsMs, maxTs = s.get(s.size() - 1).tsMs;

        double maxMb = s.stream().mapToDouble(ro -> ro.usedMb).max().orElse(1.0);
        double[] ntY = niceTicks(0, maxMb * 1.1, 6);

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);

        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Memory Usage Over Time (MB)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title)) / 2, 50);

        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " MB");
        drawXTimeTicks(g, l, t, plotW, plotH, minTs, maxTs);

        g.setColor(Color.BLACK);
        int prevX = -1, prevY = -1;
        for (Row rw : s) {
            double xS = (rw.tsMs - minTs) / 1000.0;
            double maxS = (maxTs - minTs) / 1000.0;
            int x = l + (int) Math.round((xS / Math.max(1e-9, maxS)) * plotW);
            int y = t + (int) Math.round((1.0 - (rw.usedMb - ntY[0]) / (ntY[1] - ntY[0])) * plotH);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            g.fillOval(x - 2, y - 2, 4, 4);
            prevX = x; prevY = y;
        }

        drawXAxisTitle(g, "Time (s)", l, t, plotW, plotH);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyVsCpu(List<Row> rows, Path outPng) throws IOException {
        int w = 1000, h = 700, l = 110, r = 50, t = 100, b = 100;
        int plotW = w - l - r, plotH = h - t - b;

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);

        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Latency vs CPU (%)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title)) / 2, 50);

        double maxY = rows.stream().mapToDouble(ro -> ro.latencyMs).max().orElse(1.0);
        double[] ntY = niceTicks(0, maxY * 1.05, 6);
        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " ms");

        double[] ntX = niceTicks(0, 100, 6);
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();
        for (double v = ntX[0]; v <= ntX[1] + 1e-12; v += ntX[2]) {
            int x = l + (int) Math.round(((v - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
            g.setColor(new Color(220, 220, 220)); g.drawLine(x, t, x, t + plotH);
            String lbl = String.format(Locale.ROOT, "%.0f %%", v);
            int tw = fm.stringWidth(lbl); g.setColor(Color.BLACK);
            g.drawString(lbl, x - tw / 2, t + plotH + fm.getAscent() * 2 + 6);
        }

        g.setColor(Color.BLACK);
        for (Row ro : rows) {
            if (Double.isNaN(ro.cpuPct) || ro.cpuPct < 0) continue;
            int x = l + (int) Math.round(((ro.cpuPct - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
            int y = t + (int) Math.round((1.0 - (ro.latencyMs - ntY[0]) / (ntY[1] - ntY[0])) * plotH);
            g.fillOval(x - 2, y - 2, 4, 4);
        }

        drawXAxisTitle(g, "CPU (%)", l, t, plotW, plotH);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyVsMemory(List<Row> rows, Path outPng) throws IOException {
        int w = 1000, h = 700, l = 110, r = 50, t = 100, b = 100;
        int plotW = w - l - r, plotH = h - t - b;

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);

        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Latency vs Memory (MB)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title)) / 2, 50);

        double maxY = rows.stream().mapToDouble(ro -> ro.latencyMs).max().orElse(1.0);
        double[] ntY = niceTicks(0, maxY * 1.05, 6);
        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " ms");

        double minX = rows.stream().mapToDouble(ro -> ro.usedMb).min().orElse(0);
        double maxX = rows.stream().mapToDouble(ro -> ro.usedMb).max().orElse(1);
        double[] ntX = niceTicks(minX, maxX, 6);

        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();
        for (double v = ntX[0]; v <= ntX[1] + 1e-12; v += ntX[2]) {
            int x = l + (int) Math.round(((v - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
            g.setColor(new Color(220, 220, 220)); g.drawLine(x, t, x, t + plotH);
            String lbl = String.format(Locale.ROOT, "%.0f MB", v);
            int tw = fm.stringWidth(lbl); g.setColor(Color.BLACK);
            g.drawString(lbl, x - tw / 2, t + plotH + fm.getAscent() * 2 + 6);
        }

        g.setColor(Color.BLACK);
        for (Row ro : rows) {
            int x = l + (int) Math.round(((ro.usedMb - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
            int y = t + (int) Math.round((1.0 - (ro.latencyMs - ntY[0]) / (ntY[1] - ntY[0])) * plotH);
            g.fillOval(x - 2, y - 2, 4, 4);
        }

        drawXAxisTitle(g, "Used memory (MB)", l, t, plotW, plotH);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotThroughputVsThreads(Path throughputCsv, Path outPng) throws IOException {
        if (!Files.exists(throughputCsv)) return;

        List<Integer> threads = new ArrayList<>();
        List<Double> ops = new ArrayList<>();
        try (Reader r = Files.newBufferedReader(throughputCsv, StandardCharsets.UTF_8);
             CSVParser p = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim().parse(r)) {
            for (CSVRecord rec : p) {
                threads.add(Integer.parseInt(rec.get("threads")));
                ops.add(Double.parseDouble(rec.get("ops_per_sec")));
            }
        }
        if (threads.isEmpty()) return;

        int w = 1000, h = 720, l = 110, r = 50, t = 100, b = 140;
        int plotW = w - l - r, plotH = h - t - b;

        double maxOps = ops.stream().mapToDouble(Double::doubleValue).max().orElse(1.0);
        double[] ntY = niceTicks(0, maxOps * 1.15, 6);

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0, 0, w, h);

        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 22f));
        String title = "Search — Throughput vs Threads (ops/s)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title)) / 2, 50);

        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " ops/s");

        g.setColor(brandBlue());
        g.setStroke(new BasicStroke(2.5f));
        int n = threads.size();
        int prevX = -1, prevY = -1;
        for (int i = 0; i < n; i++) {
            int x = l + (int) Math.round((i / (double) (n - 1)) * plotW);
            int y = t + (int) Math.round((1.0 - (ops.get(i) / ntY[1])) * plotH);
            g.fillOval(x - 4, y - 4, 8, 8);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;

            g.setFont(g.getFont().deriveFont(Font.PLAIN, 14f));
            String lbl = threads.get(i) + "";
            int tw = g.getFontMetrics().stringWidth(lbl);
            g.setColor(Color.BLACK);
            g.drawString(lbl, x - tw / 2, t + plotH + 30);
            g.setColor(brandBlue());
        }

        g.setFont(g.getFont().deriveFont(Font.BOLD, 16f));
        String xl = "Threads";
        int xlw = g.getFontMetrics().stringWidth(xl);
        g.setColor(Color.DARK_GRAY);
        g.drawString(xl, l + (plotW - xlw) / 2, h - 30);

        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void writeThroughputEfficiency(Path throughputCsv, Path outCsv) throws IOException {
        if (!Files.exists(throughputCsv)) return;

        List<CSVRecord> recs;
        try (Reader r = Files.newBufferedReader(throughputCsv, StandardCharsets.UTF_8);
             CSVParser p = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(r)) {
            recs = p.getRecords();
        }
        if (recs.isEmpty()) return;

        double base = Double.parseDouble(recs.get(0).get("ops_per_sec"));
        StringBuilder sb = new StringBuilder("threads,ops_per_sec,speedup,efficiency\n");
        for (CSVRecord rec : recs) {
            int t = Integer.parseInt(rec.get("threads"));
            double v = Double.parseDouble(rec.get("ops_per_sec"));
            double speedup = v / base;
            double eff = speedup / t;
            sb.append(t).append(',')
                    .append(String.format(Locale.ROOT, "%.3f", v)).append(',')
                    .append(String.format(Locale.ROOT, "%.3f", speedup)).append(',')
                    .append(String.format(Locale.ROOT, "%.3f", eff)).append('\n');
        }
        Files.writeString(outCsv, sb.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("Created CSV: " + outCsv.toAbsolutePath());
    }

    static void plotSpeedupEfficiency(Path csv, Path out) throws IOException {
        if (!Files.exists(csv)) return;
        List<Integer> thr = new ArrayList<>();
        List<Double> spd = new ArrayList<>();
        List<Double> eff = new ArrayList<>();
        try (Reader r = Files.newBufferedReader(csv, StandardCharsets.UTF_8);
             CSVParser p = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim().parse(r)) {
            for (CSVRecord rec : p) {
                int t = Integer.parseInt(rec.get("threads"));
                double v = Double.parseDouble(rec.get("ops_per_sec"));
                thr.add(t);
                spd.add(v);
            }
        }
        if (thr.isEmpty()) return;
        double base = spd.get(0);
        for (int i = 0; i < spd.size(); i++) eff.add((spd.get(i) / base) / thr.get(i));
        for (int i = 0; i < spd.size(); i++) spd.set(i, spd.get(i) / base);
        int w = 1000, h = 700, l = 110, r = 50, t = 100, b = 120, plotW = w - l - r, plotH = h - t - b;
        double maxY = Math.max(spd.stream().mapToDouble(Double::doubleValue).max().orElse(1.0), 1.0);
        double[] ntY = niceTicks(0, maxY * 1.1, 6);
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        aa(g);
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, w, h);
        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 22f));
        g.drawString("Speedup / Efficiency", (w - g.getFontMetrics().stringWidth("Speedup / Efficiency")) / 2, 50);
        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, "");
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        for (int i = 0; i < thr.size(); i++) {
            int x = l + (int) Math.round((i / (double) (thr.size() - 1)) * plotW);
            g.setColor(new Color(220, 220, 220));
            g.drawLine(x, t, x, t + plotH);
            String lbl = String.valueOf(thr.get(i));
            int tw = g.getFontMetrics().stringWidth(lbl);
            g.setColor(Color.BLACK);
            g.drawString(lbl, x - tw / 2, t + plotH + g.getFontMetrics().getAscent() * 2 + 6);
        }
        int prevX = -1, prevYS = -1, prevYE = -1;
        for (int i = 0; i < thr.size(); i++) {
            int x = l + (int) Math.round((i / (double) (thr.size() - 1)) * plotW);
            int yS = t + (int) Math.round((1.0 - (spd.get(i) - ntY[0]) / (ntY[1] - ntY[0])) * plotH);
            int yE = t + (int) Math.round((1.0 - (eff.get(i) - ntY[0]) / (ntY[1] - ntY[0])) * plotH);
            g.setStroke(new BasicStroke(2.5f));
            g.setColor(brandBlue());
            if (prevX >= 0) g.drawLine(prevX, prevYS, x, yS);
            g.fillOval(x - 4, yS - 4, 8, 8);
            g.setColor(new Color(220, 20, 60));
            if (prevX >= 0) g.drawLine(prevX, prevYE, x, yE);
            g.fillOval(x - 4, yE - 4, 8, 8);
            prevX = x;
            prevYS = yS;
            prevYE = yE;
        }
        drawLegend(g, List.of("Speedup", "Efficiency"), List.of(brandBlue(), new Color(220, 20, 60)), l + 20, t - 20);
        drawXAxisTitle(g, "Threads", l, t, plotW, plotH);
        ImageIO.write(img, "png", out.toFile());
        g.dispose();
    }
}
