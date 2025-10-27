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
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PlotIngestionBench {

    private static final String BENCH = "ingestion";

    public static void main(String[] args) throws Exception {
        Path base     = Path.of("benchmarking_results").resolve(BENCH);
        Path dataDir  = base.resolve("data");
        Path plotsDir = base.resolve("plots");
        Files.createDirectories(plotsDir);

        Path preferred = (args.length > 0) ? Path.of(args[0]) : dataDir;
        if (Files.isDirectory(preferred)) dataDir = preferred;

        List<Row> rows = loadBestAvailableRows(dataDir);

        if (rows.isEmpty()) {
            System.out.println("No se encontraron filas de throughput en ningún CSV dentro de: " + dataDir.toAbsolutePath());
            return;
        }

        List<Row> thrpt = rows.stream()
                .filter(r -> "thrpt".equals(r.mode) && "iteration".equals(r.phase))
                .collect(Collectors.toList());

        if (thrpt.isEmpty()) {
            thrpt = rows.stream()
                    .filter(r -> "thrpt".equals(r.mode))
                    .collect(Collectors.toList());
            thrpt = thrpt.stream().map(r -> new Row(r.threads, "iteration", r.iteration, r.value, r.unit, r.mode)).toList();
        }

        if (thrpt.isEmpty()) {
            System.out.println("No throughput rows found (ni en iteraciones ni en agregados).");
            return;
        }

        Map<Integer, List<Row>> byThreads = thrpt.stream()
                .collect(Collectors.groupingBy(r -> r.threads, TreeMap::new, Collectors.toList()));

        Map<Integer, Stats> agg = aggregateOps(byThreads);
        writeSummaryThrpt(agg, dataDir.resolve(BENCH + "_summary_throughput.csv"));

        plotThroughputVsThreadsEnhanced(agg, plotsDir.resolve(BENCH + "_throughput_vs_threads.png"));
        plotSpeedupVsThreads(agg, plotsDir.resolve(BENCH + "_speedup_vs_threads.png"));

        System.out.println("Plots in: " + plotsDir.toAbsolutePath());
    }

    private static List<Row> loadBestAvailableRows(Path dataDir) throws IOException {
        Path merged = dataDir.resolve(BENCH + "_data.csv");
        if (hasData(merged)) {
            List<Row> r = loadIterationsCsv(merged);
            if (!r.isEmpty()) return r;
        }

        List<Path> iterationParts = listMatching(dataDir, "ingestion_iterations_t*.csv");
        List<Row> iterAll = new ArrayList<>();
        for (Path p : iterationParts) if (hasData(p)) iterAll.addAll(loadIterationsCsv(p));
        if (!iterAll.isEmpty()) return iterAll;

        List<Path> aggs = new ArrayList<>(listMatching(dataDir, "ingestion_t*.csv"));
        Path aggMerged = dataDir.resolve(BENCH + "_agg.csv");
        if (Files.exists(aggMerged)) aggs.add(0, aggMerged);

        List<Row> fromAgg = new ArrayList<>();
        for (Path p : aggs) if (hasData(p)) fromAgg.addAll(loadAggCsvAsThroughputIterations(p));
        return fromAgg;
    }

    private static boolean hasData(Path p) {
        try {
            if (!Files.exists(p)) return false;
            try (Stream<String> s = Files.lines(p)) {
                return s.skip(1).anyMatch(line -> !line.isBlank());
            }
        } catch (IOException e) {
            return false;
        }
    }

    private static List<Path> listMatching(Path dir, String glob) throws IOException {
        if (!Files.isDirectory(dir)) return List.of();
        try (Stream<Path> s = Files.list(dir)) {
            PathMatcher m = FileSystems.getDefault().getPathMatcher("glob:" + glob);
            return s.filter(Files::isRegularFile)
                    .filter(p -> m.matches(p.getFileName()))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private static List<Row> loadIterationsCsv(Path csv) throws IOException {
        try (Reader r = Files.newBufferedReader(csv, StandardCharsets.UTF_8);
             CSVParser p = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim().parse(r)) {
            List<Row> out = new ArrayList<>();
            for (CSVRecord rec : p) {
                String mode = getLower(rec, "mode");
                String phase = getLower(rec, "phase");
                if (mode == null || phase == null) continue;
                if (!mode.contains("thrpt")) continue;

                Integer threads = parseInt(rec, "threads");
                Integer iteration = parseInt(rec, "iteration");
                Double value = parseDouble(rec, "value");
                String unit = getLower(rec, "unit");

                if (threads == null || value == null) continue;
                if (iteration == null) iteration = 1;
                if (unit == null) unit = "ops/s";

                out.add(new Row(threads, phase, iteration, value, unit, "thrpt"));
            }
            return out;
        }
    }

    private static List<Row> loadAggCsvAsThroughputIterations(Path csv) throws IOException {
        try (Reader r = Files.newBufferedReader(csv, StandardCharsets.UTF_8);
             CSVParser p = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim().parse(r)) {
            List<Row> out = new ArrayList<>();
            for (CSVRecord rec : p) {
                String mode = getLower(rec, "Mode");
                if (mode == null) mode = getLower(rec, "mode");
                if (mode == null) continue;
                if (!(mode.contains("thrpt") || mode.contains("throughput"))) continue;

                Integer threads = parseInt(rec, "Threads");
                if (threads == null) threads = parseInt(rec, "threads");
                if (threads == null) continue;

                Double score = parseDouble(rec, "Score");
                if (score == null) score = parseDouble(rec, "score");
                if (score == null) continue;

                String unit = getLower(rec, "Unit");
                if (unit == null) unit = getLower(rec, "unit");
                if (unit == null) unit = "ops/s";

                out.add(new Row(threads, "iteration", 1, score, unit, "thrpt"));
            }
            return out;
        }
    }

    private static String getLower(CSVRecord rec, String key) {
        try {
            String v = rec.get(key);
            return (v == null) ? null : v.trim().toLowerCase(Locale.ROOT);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static Integer parseInt(CSVRecord rec, String key) {
        try {
            String s = rec.get(key);
            if (s == null || s.trim().isEmpty()) return null;
            return Integer.parseInt(s.replace(",", "").trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static Double parseDouble(CSVRecord rec, String key) {
        try {
            String s = rec.get(key);
            if (s == null || s.trim().isEmpty()) return null;
            return Double.parseDouble(s.replace(",", "").trim());
        } catch (Exception e) {
            return null;
        }
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

    static void enableAA(Graphics2D g) {
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
    }

    static void drawTextWithBg(Graphics2D g, String text, int x, int y) {
        FontMetrics fm = g.getFontMetrics();
        int w = fm.stringWidth(text);
        int h = fm.getAscent();
        g.setColor(Color.WHITE);
        g.fillRoundRect(x - 4, y - h, w + 8, h + 6, 6, 6);
        g.setColor(Color.BLACK);
        g.drawString(text, x, y);
    }

    static int minDelta(List<Integer> xs) {
        int min = Integer.MAX_VALUE;
        for (int i = 1; i < xs.size(); i++)
            min = Math.min(min, xs.get(i) - xs.get(i - 1));
        return (min == Integer.MAX_VALUE) ? 0 : min;
    }

    static void plotThroughputVsThreadsEnhanced(Map<Integer, Stats> agg, Path outPng) throws IOException {
        int w = 1100, h = 700, l = 100, r = 60, t = 100, b = 100;
        int plotW = w - l - r, plotH = h - t - b;

        List<Integer> xs = new ArrayList<>(agg.keySet());
        Collections.sort(xs);

        double maxY = agg.values().stream().mapToDouble(s -> s.mean + s.std).max().orElse(1.0) * 1.12;
        double minY = 0;

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        enableAA(g);

        g.setColor(Color.WHITE);
        g.fillRect(0, 0, w, h);

        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        g.setColor(new Color(0,102,204));
        String title = "Ingestion — Throughput vs Threads (books/s)";
        FontMetrics fmt = g.getFontMetrics();
        g.drawString(title, (w - fmt.stringWidth(title))/2, 50);

        drawYAxisOps(g, l, t, plotW, plotH, minY, maxY, 6);
        drawXTicksThreads(g, l, t, plotW, plotH, xs);

        g.setColor(new Color(0,102,204));
        g.setStroke(new BasicStroke(2f));
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 14f));
        FontMetrics fm = g.getFontMetrics();

        double[] nt = niceTicks(minY, maxY, 6);
        double y0 = nt[0], y1 = nt[1];

        int prevX = -1, prevY = -1;
        for (int i = 0; i < xs.size(); i++) {
            int thr = xs.get(i);
            Stats s = agg.get(thr);
            int x = xForIndex(i, xs.size(), l, plotW);
            int y = t + (int) Math.round((1.0 - (s.mean - y0) / (y1 - y0)) * plotH);

            g.fillOval(x - 4, y - 4, 8, 8);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;

            String label = fmtOps(s.mean);
            int lx = x - fm.stringWidth(label)/2;
            int ly = Math.max(t + 16, y - 12);
            g.setColor(Color.WHITE);
            g.fillRoundRect(lx - 4, ly - fm.getAscent(), fm.stringWidth(label) + 8, fm.getHeight(), 6, 6);
            g.setColor(Color.BLACK);
            g.drawString(label, lx, ly);
            g.setColor(new Color(0,102,204)); // volver al azul para la línea
        }

        g.setColor(Color.DARK_GRAY);
        g.setFont(g.getFont().deriveFont(Font.BOLD, 16f));
        g.drawString("Threads", l + plotW/2 - 30, h - 40);
        g.rotate(-Math.PI/2);
        g.drawString("Throughput (books/s)", -(t + plotH/2 + 40), 40);
        g.rotate(Math.PI/2);

        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void plotSpeedupVsThreads(Map<Integer, Stats> agg, Path outPng) throws IOException {
        if (!agg.containsKey(1)) return;

        int w = 1100, h = 700, l = 100, r = 60, t = 100, b = 100;
        int plotW = w - l - r, plotH = h - t - b;

        List<Integer> xs = new ArrayList<>(agg.keySet());
        Collections.sort(xs);

        double base = agg.get(1).mean;
        Map<Integer, Double> speedup = xs.stream()
                .collect(Collectors.toMap(i -> i, i -> agg.get(i).mean / base, (a, b1) -> a, TreeMap::new));

        double maxY = speedup.values().stream().mapToDouble(Double::doubleValue).max().orElse(1.0) * 1.2;
        double minY = 0;

        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        enableAA(g);
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, w, h);

        g.setFont(g.getFont().deriveFont(Font.BOLD, 20f));
        g.setColor(new Color(0, 102, 204));
        String title = "Ingestion — Speedup vs Threads";
        FontMetrics fmTitle = g.getFontMetrics();
        g.drawString(title, (w - fmTitle.stringWidth(title)) / 2, 50);

        drawYAxisLinear(g, l, t, plotW, plotH, minY, maxY, 6, "×");
        drawXTicksThreads(g, l, t, plotW, plotH, xs);

        g.setColor(new Color(0, 102, 204));
        g.setStroke(new BasicStroke(2f));

        int prevX = -1, prevY = -1;
        FontMetrics fm = g.getFontMetrics();
        for (int i = 0; i < xs.size(); i++) {
            int thr = xs.get(i);
            double sp = speedup.get(thr);
            int x = xForIndex(i, xs.size(), l, plotW);
            int y = t + (int) ((1 - (sp - minY) / (maxY - minY)) * plotH);
            g.fillOval(x - 4, y - 4, 8, 8);
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x; prevY = y;

            String lbl = String.format(Locale.ROOT, "×%.2f", sp);
            drawTextWithBg(g, lbl, x - fm.stringWidth(lbl) / 2, y - 14);
        }

        g.setColor(Color.DARK_GRAY);
        g.setFont(g.getFont().deriveFont(Font.BOLD, 16f));
        g.drawString("Threads", l + plotW / 2 - 30, h - 40);
        g.rotate(-Math.PI / 2);
        g.drawString("Speedup", -(t + plotH / 2 + 40), 40);
        g.rotate(Math.PI / 2);

        ImageIO.write(img, "png", outPng.toFile());
        g.dispose();
    }

    static void drawXTicksThreads(Graphics2D g, int l, int t, int plotW, int plotH, List<Integer> xs) {
        g.setFont(g.getFont().deriveFont(Font.PLAIN, 12f));
        FontMetrics fm = g.getFontMetrics();
        List<Integer> xPixels = new ArrayList<>();
        for (int i = 0; i < xs.size(); i++) xPixels.add(xForIndex(i, xs.size(), l, plotW));
        boolean rotate = minDelta(xPixels) < (fm.stringWidth("00") + 8);
        for (int i = 0; i < xs.size(); i++) {
            int x = xPixels.get(i);
            g.drawLine(x, t + plotH, x, t + plotH + 8);
            String lbl = xs.get(i).toString();
            if (!rotate)
                g.drawString(lbl, x - fm.stringWidth(lbl) / 2, t + plotH + fm.getAscent() + 10);
            else {
                Graphics2D g2 = (Graphics2D) g.create();
                int baseY = t + plotH + fm.getAscent() + 6;
                g2.rotate(Math.toRadians(45), x, baseY);
                drawTextWithBg(g2, lbl, x - fm.stringWidth(lbl) / 2, baseY);
                g2.dispose();
            }
        }
    }

    static void drawYAxisOps(Graphics2D g, int l, int t, int plotW, int plotH,
                             double minY, double maxY, int ticks) {
        g.setColor(Color.BLACK);
        g.drawLine(l, t, l, t + plotH);
        g.drawLine(l, t + plotH, l + plotW, t + plotH);

        double[] nt = niceTicks(minY, maxY, Math.max(3, ticks));
        double y0 = nt[0], y1 = nt[1], step = nt[2];

        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();

        for (double v = y0; v <= y1 + 1e-12; v += step) {
            int y = t + (int) Math.round((1.0 - (v - y0) / (y1 - y0)) * plotH);

            g.setColor(new Color(230,230,230));
            g.drawLine(l, y, l + plotW, y);

            g.setColor(Color.BLACK);
            g.drawLine(l - 4, y, l, y);

            String label = fmtOps(v) + " ops/s";
            int tw = fm.stringWidth(label);
            int ty = y + fm.getAscent()/2 - 2;

            g.setColor(Color.WHITE);
            g.fillRoundRect(l - 10 - tw - 4, ty - fm.getAscent(), tw + 8, fm.getHeight(), 6, 6);
            g.setColor(Color.BLACK);
            g.drawString(label, l - 10 - tw, ty);
        }
    }

    static void drawYAxisLinear(Graphics2D g, int l, int t, int plotW, int plotH,
                                double minY, double maxY, int ticks, String suffix) {
        g.setColor(Color.BLACK);
        g.drawLine(l, t, l, t + plotH);               // eje Y
        g.drawLine(l, t + plotH, l + plotW, t + plotH); // eje X

        double[] nt = niceTicks(minY, maxY, Math.max(3, ticks));
        double y0 = nt[0], y1 = nt[1], step = nt[2];

        g.setFont(g.getFont().deriveFont(Font.PLAIN, 13f));
        FontMetrics fm = g.getFontMetrics();

        for (double v = y0; v <= y1 + 1e-12; v += step) {
            int y = t + (int) Math.round((1.0 - (v - y0) / (y1 - y0)) * plotH);

            g.setColor(new Color(230,230,230));
            g.drawLine(l, y, l + plotW, y);

            g.setColor(Color.BLACK);
            g.drawLine(l - 4, y, l, y);

            String label = (suffix == null)
                    ? String.format(Locale.ROOT, "%.2f", v)
                    : String.format(Locale.ROOT, "%.2f%s", v, suffix);
            int tw = fm.stringWidth(label);
            int ty = y + fm.getAscent()/2 - 2;

            g.setColor(Color.WHITE);
            g.fillRoundRect(l - 10 - tw - 4, ty - fm.getAscent(), tw + 8, fm.getHeight(), 6, 6);
            g.setColor(Color.BLACK);
            g.drawString(label, l - 10 - tw, ty);
        }
    }

    static double[] niceTicks(double min, double max, int maxTicks) {
        double range = niceNum(max - min, false);
        double d = niceNum(range / (maxTicks - 1), true);
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

    static String fmtOps(double v) {
        double abs = Math.abs(v);
        if (abs >= 1_000_000_000) return String.format(Locale.ROOT, "%.2fG", v / 1_000_000_000d);
        if (abs >= 1_000_000) return String.format(Locale.ROOT, "%.2fM", v / 1_000_000d);
        if (abs >= 1_000) return String.format(Locale.ROOT, "%.2fk", v / 1_000d);
        return String.format(Locale.ROOT, "%.0f", v);
    }

    static int xForIndex(int i, int count, int l, int plotW) {
        return l + (count <= 1 ? 0 : (int) Math.round(i * (plotW / (count - 1.0))));
    }

    static Color[] palette() {
        return new Color[]{
                new Color(30,144,255),
                new Color(46,204,113),
                new Color(231,76,60),
                new Color(52,73,94)
        };
    }

    static Map<Integer, Stats> aggregateOps(Map<Integer, List<Row>> thrByThreads) {
        Map<Integer, Stats> out = new TreeMap<>();
        for (var e : thrByThreads.entrySet()) {
            double[] vals = e.getValue().stream().mapToDouble(r -> r.value).toArray();
            double mean = Arrays.stream(vals).average().orElse(0);
            double std = Math.sqrt(Arrays.stream(vals).map(v -> Math.pow(v - mean, 2)).sum() / vals.length);
            out.put(e.getKey(), new Stats(mean, std));
        }
        return out;
    }

    static void writeSummaryThrpt(Map<Integer, Stats> thrAgg, Path csv) throws IOException {
        DecimalFormat df = new DecimalFormat("#.####");
        StringBuilder sb = new StringBuilder("threads,mean_ops_s,std_ops_s,efficiency\n");
        double base1 = thrAgg.containsKey(1) ? thrAgg.get(1).mean : -1;
        for (var e : thrAgg.entrySet()) {
            double eff = (base1 > 0) ? (e.getValue().mean / (base1 * e.getKey())) : Double.NaN;
            sb.append(e.getKey()).append(',')
                    .append(df.format(e.getValue().mean)).append(',')
                    .append(df.format(e.getValue().std)).append(',')
                    .append(Double.isNaN(eff) ? "" : df.format(eff)).append('\n');
        }
        Files.writeString(csv, sb.toString(), StandardCharsets.UTF_8);
    }
}
