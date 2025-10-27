package com.tahs.analysis;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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
        if (rows.isEmpty()) {
            System.out.println("No samples in: " + samplesCsv.toAbsolutePath());
            return;
        }
        writeOverallSummary(rows, DATA.resolve("search_summary_overall.csv"));
        writeByTermSummary(rows, DATA.resolve("search_summary_by_term.csv"));
        plotLatencyHistogram(rows, PLOTS.resolve("search_latency_histogram.png"));
        plotLatencyCDF(rows, PLOTS.resolve("search_latency_cdf.png"));
        plotLatencyOverTime(rows, PLOTS.resolve("search_latency_over_time.png"));
        plotLatencyByTermP50P95(rows, PLOTS.resolve("search_latency_p50_p95_by_term.png"));
        System.out.println("CSV summaries in: " + DATA.toAbsolutePath());
        System.out.println("Plots in: " + PLOTS.toAbsolutePath());
    }

    static final class Row {
        final long tsMs;
        final String term;
        final double latencyMs;
        final double cpuPct;
        final double usedMb;
        final double totalMb;
        Row(long tsMs, String term, double latencyMs, double cpuPct, double usedMb, double totalMb) {
            this.tsMs = tsMs; this.term = term; this.latencyMs = latencyMs; this.cpuPct = cpuPct; this.usedMb = usedMb; this.totalMb = totalMb;
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
        try { return Double.parseDouble(rec.get(key).trim()); } catch (Exception e) { return Double.NaN; }
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

    static void writeOverallSummary(List<Row> rows, Path csv) throws IOException {
        double[] lat = latencies(rows);
        DecimalFormat df = new DecimalFormat("#.###");
        String out = "count,avg_ms,p50_ms,p95_ms\n" +
                lat.length + "," + df.format(mean(lat)) + "," +
                df.format(percentile(lat, 50)) + "," +
                df.format(percentile(lat, 95)) + "\n";
        Files.writeString(csv, out, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
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
        Files.writeString(csv, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    static void aa(Graphics2D g) {
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
    }

    static Color brandBlue() { return new Color(0,102,204); }

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
        double nf;
        if (round) nf = (f < 1.5) ? 1 : (f < 3) ? 2 : (f < 7) ? 5 : 10;
        else nf = (f <= 1) ? 1 : (f <= 2) ? 2 : (f <= 5) ? 5 : 10;
        return nf * Math.pow(10, exp);
    }

    static void drawYLinear(Graphics2D g, int l, int t, int plotW, int plotH, double minY, double maxY, int ticks, String unitSuffix) {
        g.setColor(Color.BLACK);
        g.drawLine(l, t - 6, l, t + plotH + 6);
        g.drawLine(l, t + plotH, l + plotW + 6, t + plotH);
        double[] nt = niceTicks(minY, maxY, Math.max(3, ticks));
        double y0 = nt[0], y1 = nt[1], step = nt[2];
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();
        for (double v = y0; v <= y1 + 1e-12; v += step) {
            int y = t + (int) Math.round((1.0 - (v - y0) / (y1 - y0)) * plotH);
            g.setColor(new Color(230,230,230));
            g.drawLine(l, y, l + plotW, y);
            String label = (unitSuffix == null) ? String.format(Locale.ROOT, "%.2f", v) : String.format(Locale.ROOT, "%.2f%s", v, unitSuffix);
            int tw = fm.stringWidth(label);
            int ty = y + fm.getAscent()/2 - 2;
            g.setColor(Color.WHITE);
            g.fillRoundRect(l - 12 - tw, ty - fm.getAscent(), tw + 8, fm.getHeight(), 6, 6);
            g.setColor(Color.BLACK);
            g.drawString(label, l - 8 - tw, ty);
            g.setColor(Color.BLACK);
            g.drawLine(l - 4, y, l, y);
        }
    }

    static void drawXTicksTerms(Graphics2D g, int l, int t, int plotW, int plotH, List<String> terms) {
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 12f));
        FontMetrics fm = g.getFontMetrics();
        int n = terms.size();
        for (int i = 0; i < n; i++) {
            int x = l + (int) Math.round((i + 0.5) * (plotW / (double) n));
            g.setColor(Color.BLACK);
            g.drawLine(x, t + plotH, x, t + plotH + 6);
            String lbl = terms.get(i);
            int tw = fm.stringWidth(lbl);
            Graphics2D g2 = (Graphics2D) g.create();
            int baseY = t + plotH + fm.getAscent() + 6;
            g2.rotate(Math.toRadians(45), x, baseY);
            g2.setColor(Color.WHITE);
            g2.fillRoundRect(x - tw/2 - 4, baseY - fm.getAscent(), tw + 8, fm.getHeight(), 6, 6);
            g2.setColor(Color.BLACK);
            g2.drawString(lbl, x - tw/2, baseY);
            g2.dispose();
        }
    }

    static void plotLatencyHistogram(List<Row> rows, Path outPng) throws IOException {
        int w = 1100, h = 700, l = 110, r = 50, t = 100, b = 100;
        int plotW = w - l - r, plotH = h - t - b;
        double[] lat = latencies(rows);
        if (lat.length == 0) return;
        int bins = Math.min(50, Math.max(20, lat.length / 100));
        double max = lat[lat.length - 1];
        if (max <= 0) max = 1;
        int[] counts = new int[bins];
        for (double v : lat) {
            int i = (int) Math.floor((v / max) * (bins - 1));
            if (i < 0) i = 0; if (i >= bins) i = bins - 1;
            counts[i]++;
        }
        int maxC = Arrays.stream(counts).max().orElse(1);
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        aa(g); g.setColor(Color.WHITE); g.fillRect(0,0,w,h);
        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Search — Latency Histogram (ms)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title))/2, 50);
        drawYLinear(g, l, t, plotW, plotH, 0, maxC, 6, "");
        double[] ntX = niceTicks(0, max, 6);
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();
        for (double v = ntX[0]; v <= ntX[1] + 1e-12; v += ntX[2]) {
            int x = l + (int) Math.round(((v - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
            g.setColor(new Color(220,220,220));
            g.drawLine(x, t, x, t + plotH);
            String lbl = String.format(Locale.ROOT, "%.0f ms", v);
            int tw = fm.stringWidth(lbl);
            g.setColor(Color.WHITE);
            g.fillRoundRect(x - tw/2 - 4, t + plotH + fm.getAscent(), tw + 8, fm.getHeight(), 6, 6);
            g.setColor(Color.BLACK);
            g.drawString(lbl, x - tw/2, t + plotH + fm.getAscent()*2 + 6);
        }
        int barW = Math.max(1, plotW / bins);
        g.setColor(brandBlue());
        for (int i = 0; i < bins; i++) {
            int hBar = (int) Math.round((counts[i] / (double) maxC) * plotH);
            int x = l + i * barW;
            int y = t + plotH - hBar;
            g.fillRect(x, y, Math.max(1, barW - 2), hBar);
        }
        double p50 = percentile(lat, 50);
        double p95 = percentile(lat, 95);
        int x50 = l + (int) Math.round(((p50 - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
        int x95 = l + (int) Math.round(((p95 - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
        g.setStroke(new BasicStroke(2f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1f, new float[]{6f,6f}, 0f));
        g.setColor(new Color(0,0,0,120));
        g.drawLine(x50, t, x50, t + plotH);
        g.drawLine(x95, t, x95, t + plotH);
        g.setFont(g.getFont().deriveFont(Font.BOLD, 14f));
        g.setColor(Color.BLACK);
        g.drawString("P50 ≈ " + Math.round(p50) + " ms", x50 + 8, t + 20);
        g.drawString("P95 ≈ " + Math.round(p95) + " ms", x95 + 8, t + 38);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyCDF(List<Row> rows, Path outPng) throws IOException {
        int w = 1100, h = 700, l = 110, r = 50, t = 100, b = 100;
        int plotW = w - l - r, plotH = h - t - b;
        double[] lat = latencies(rows);
        if (lat.length == 0) return;
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        aa(g); g.setColor(Color.WHITE); g.fillRect(0,0,w,h);
        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Search — Latency CDF (ms)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title))/2, 50);
        double maxX = lat[lat.length-1];
        double[] ntX = niceTicks(0, maxX, 6);
        drawYLinear(g, l, t, plotW, plotH, 0, 1.0, 6, "");
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();
        for (double v = ntX[0]; v <= ntX[1] + 1e-12; v += ntX[2]) {
            int x = l + (int) Math.round(((v - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
            g.setColor(new Color(220,220,220));
            g.drawLine(x, t, x, t + plotH);
            String lbl = String.format(Locale.ROOT, "%.0f ms", v);
            int tw = fm.stringWidth(lbl);
            g.setColor(Color.WHITE);
            g.fillRoundRect(x - tw/2 - 4, t + plotH + fm.getAscent(), tw + 8, fm.getHeight(), 6, 6);
            g.setColor(Color.BLACK);
            g.drawString(lbl, x - tw/2, t + plotH + fm.getAscent()*2 + 6);
        }
        g.setColor(brandBlue());
        g.setStroke(new BasicStroke(2.2f));
        int prevX = -1, prevY = -1;
        for (int i = 0; i < lat.length; i++) {
            double xVal = lat[i];
            double yVal = (lat.length == 1) ? 1.0 : (i / (double)(lat.length - 1));
            int x = l + (int) Math.round(((xVal - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
            int y = t + (int) Math.round((1.0 - yVal) * plotH);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }
        double p50 = percentile(lat, 50);
        double p95 = percentile(lat, 95);
        int x50 = l + (int) Math.round(((p50 - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
        int x95 = l + (int) Math.round(((p95 - ntX[0]) / (ntX[1] - ntX[0])) * plotW);
        g.setStroke(new BasicStroke(2f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 1f, new float[]{6,6}, 0));
        g.setColor(new Color(0,0,0,120));
        g.drawLine(x50, t, x50, t + plotH);
        g.drawLine(x95, t, x95, t + plotH);
        g.setFont(g.getFont().deriveFont(Font.BOLD, 14f));
        g.setColor(Color.BLACK);
        g.drawString("P50 ≈ " + Math.round(p50) + " ms", x50 + 8, t + 22);
        g.drawString("P95 ≈ " + Math.round(p95) + " ms", x95 + 8, t + 40);
        g.setColor(Color.DARK_GRAY);
        g.setFont(g.getFont().deriveFont(Font.BOLD, 16f));
        g.drawString("Latency (ms)", l + plotW/2 - 40, h - 40);
        g.rotate(-Math.PI/2);
        g.drawString("Cumulative Probability", -(t + plotH/2 + 40), 40);
        g.rotate(Math.PI/2);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyOverTime(List<Row> rows, Path outPng) throws IOException {
        int w = 1200, h = 720, l = 120, r = 60, t = 100, b = 120;
        int plotW = w - l - r, plotH = h - t - b;
        List<Row> sorted = rows.stream().sorted(Comparator.comparingLong(rw -> rw.tsMs)).toList();
        long minTs = sorted.get(0).tsMs;
        long maxTs = sorted.get(sorted.size()-1).tsMs;
        if (maxTs == minTs) maxTs = minTs + 1;
        double maxY = sorted.stream().mapToDouble(ro -> ro.latencyMs).max().orElse(1.0);
        double[] ntY = niceTicks(0, maxY * 1.05, 6);
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        aa(g); g.setColor(Color.WHITE); g.fillRect(0,0,w,h);
        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Search — Latency Over Time (ms)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title))/2, 50);
        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " ms");
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();
        int ticks = 6;
        for (int i = 0; i <= ticks; i++) {
            long ts = minTs + Math.round(i * (maxTs - minTs) / (double) ticks);
            int x = l + (int) Math.round(((ts - minTs) / (double)(maxTs - minTs)) * plotW);
            g.setColor(new Color(220,220,220));
            g.drawLine(x, t, x, t + plotH);
            String lbl = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(ts));
            int tw = fm.stringWidth(lbl);
            g.setColor(Color.WHITE);
            g.fillRoundRect(x - tw/2 - 4, t + plotH + fm.getAscent(), tw + 8, fm.getHeight(), 6, 6);
            g.setColor(Color.BLACK);
            g.drawString(lbl, x - tw/2, t + plotH + fm.getAscent()*2 + 6);
        }
        g.setColor(brandBlue());
        g.setStroke(new BasicStroke(2.0f));
        int prevX = -1, prevY = -1;
        double y0 = ntY[0], y1 = ntY[1];
        for (Row rw : sorted) {
            int x = l + (int) Math.round(((rw.tsMs - minTs) / (double)(maxTs - minTs)) * plotW);
            int y = t + (int) Math.round((1.0 - (rw.latencyMs - y0) / (y1 - y0)) * plotH);
            g.fillOval(x - 2, y - 2, 4, 4);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;
        }
        g.setColor(Color.DARK_GRAY);
        g.setFont(g.getFont().deriveFont(Font.BOLD, 16f));
        g.drawString("Time", l + plotW/2 - 15, h - 40);
        g.rotate(-Math.PI/2);
        g.drawString("Latency (ms)", -(t + plotH/2 + 40), 40);
        g.rotate(Math.PI/2);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotLatencyByTermP50P95(List<Row> rows, Path outPng) throws IOException {
        int w = 1200, h = 720, l = 120, r = 60, t = 100, b = 140;
        int plotW = w - l - r, plotH = h - t - b;
        Map<String, double[]> byTerm = new TreeMap<>();
        Map<String, List<Row>> groups = rows.stream().collect(Collectors.groupingBy(ro -> ro.term, TreeMap::new, Collectors.toList()));
        double maxStat = 0;
        for (var e : groups.entrySet()) {
            double[] lat = latencies(e.getValue());
            double p50 = percentile(lat, 50);
            double p95 = percentile(lat, 95);
            byTerm.put(e.getKey(), new double[]{p50, p95});
            maxStat = Math.max(maxStat, Math.max(p50, p95));
        }
        List<String> terms = new ArrayList<>(byTerm.keySet());
        if (terms.isEmpty()) return;
        double[] ntY = niceTicks(0, maxStat * 1.15, 6);
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        aa(g); g.setColor(Color.WHITE); g.fillRect(0,0,w,h);
        g.setColor(brandBlue());
        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        String title = "Search — P50 / P95 Latency by Term (ms)";
        FontMetrics ft = g.getFontMetrics();
        g.drawString(title, (w - ft.stringWidth(title))/2, 50);
        drawYLinear(g, l, t, plotW, plotH, ntY[0], ntY[1], 6, " ms");
        int n = terms.size();
        int groupW = (int) Math.floor(plotW / (double) n);
        int barW = Math.max(12, (int) Math.floor(groupW * 0.32));
        int gap = Math.max(8, (groupW - 2*barW) / 3);
        drawXTicksTerms(g, l, t, plotW, plotH, terms);
        g.setStroke(new BasicStroke(1.2f));
        Color p50Color = brandBlue();
        Color p95Color = new Color(30,144,255);
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
            g.setColor(p50Color);
            g.fillRect(xP50, yP50, barW, hP50);
            g.setColor(Color.BLACK);
            g.drawRect(xP50, yP50, barW, hP50);
            g.setColor(p95Color);
            g.fillRect(xP95, yP95, barW, hP95);
            g.setColor(Color.BLACK);
            g.drawRect(xP95, yP95, barW, hP95);
            g.setColor(Color.BLACK);
            String l50 = Math.round(p50) + " ms";
            String l95 = Math.round(p95) + " ms";
            int tw50 = fm.stringWidth(l50), tw95 = fm.stringWidth(l95);
            drawLabel(g, l50, xP50 + barW/2 - tw50/2, Math.max(t + 18, yP50 - 8));
            drawLabel(g, l95, xP95 + barW/2 - tw95/2, Math.max(t + 18, yP95 - 8));
        }
        int lx = l + 20, ly = t - 20;
        g.setColor(p50Color); g.fillRect(lx, ly, 16, 12); g.setColor(Color.BLACK); g.drawRect(lx, ly, 16, 12);
        g.drawString("P50", lx + 22, ly + 11);
        g.setColor(p95Color); g.fillRect(lx + 70, ly, 16, 12); g.setColor(Color.BLACK); g.drawRect(lx + 70, ly, 16, 12);
        g.drawString("P95", lx + 92, ly + 11);
        g.setColor(Color.DARK_GRAY);
        g.setFont(g.getFont().deriveFont(Font.BOLD, 16f));
        g.rotate(-Math.PI/2);
        g.drawString("Latency (ms)", -(t + plotH/2 + 40), 40);
        g.rotate(Math.PI/2);
        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void drawLabel(Graphics2D g, String text, int x, int y) {
        FontMetrics fm = g.getFontMetrics();
        int tw = fm.stringWidth(text);
        g.setColor(Color.WHITE);
        g.fillRoundRect(x - 4, y - fm.getAscent(), tw + 8, fm.getHeight(), 6, 6);
        g.setColor(Color.BLACK);
        g.drawString(text, x, y);
    }
}
