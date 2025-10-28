package com.tahs.analysis;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

public class PlotIndexingBench {
    static Color B() { return new Color(0,102,204); }
    static void aa(Graphics2D g){ g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON); g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON); }
    static double niceNum(double x, boolean round){ double exp=Math.floor(Math.log10(x)); double f=x/Math.pow(10,exp); double nf= round? (f<1.5?1:(f<3?2:(f<7?5:10))) : (f<=1?1:(f<=2?2:(f<=5?5:10))); return nf*Math.pow(10,exp); }
    static double[] niceTicks(double min,double max,int maxTicks){ if(Double.isNaN(min)||Double.isNaN(max)||min==max) return new double[]{min,max,1}; double range=niceNum(max-min,false); double d=niceNum(range/Math.max(2,(maxTicks-1)),true); double niceMin=Math.floor(min/d)*d; double niceMax=Math.ceil(max/d)*d; return new double[]{niceMin,niceMax,d}; }
    static void yAxis(Graphics2D g,int l,int t,int w,int h,double y0,double y1,int ticks,String suf){ double[] nt=niceTicks(y0,y1,Math.max(3,ticks)); g.setFont(g.getFont().deriveFont(13f)); FontMetrics fm=g.getFontMetrics(); g.setColor(Color.BLACK); g.drawLine(l,t-6,l,t+h+6); g.drawLine(l,t+h,l+w+6,t+h); for(double v=nt[0]; v<=nt[1]+1e-12; v+=nt[2]){ int y=t+(int)Math.round((1.0-(v-nt[0])/(nt[1]-nt[0]))*h); g.setColor(new Color(230,230,230)); g.drawLine(l,y,l+w,y); String lab=String.format(Locale.ROOT,"%.2f%s",v,(suf==null?"":suf)); int tw=fm.stringWidth(lab); int ty=y+fm.getAscent()/2-2; g.setColor(Color.WHITE); g.fillRoundRect(l-12-tw,ty-fm.getAscent(),tw+8,fm.getHeight(),6,6); g.setColor(Color.BLACK); g.drawString(lab,l-8-tw,ty); g.setColor(Color.BLACK); g.drawLine(l-4,y,l,y); } }
    static void xTitle(Graphics2D g,String text,int l,int t,int w,int h){ g.setFont(g.getFont().deriveFont(Font.BOLD,16f)); int xx=l+(w-g.getFontMetrics().stringWidth(text))/2; g.setColor(Color.DARK_GRAY); g.drawString(text,xx,t+h+48); }

    public static void plotThroughputVsThreads(Path aggCsv, Path outPng) throws Exception {
        if(!Files.exists(aggCsv)) return;
        List<Integer> thr=new ArrayList<>(); List<Double> ops=new ArrayList<>();
        try(Reader r=Files.newBufferedReader(aggCsv, StandardCharsets.UTF_8); CSVParser p=CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim().parse(r)){
            for(CSVRecord rec:p){ String mode=rec.get("Mode"); if(!mode.equalsIgnoreCase("thrpt")) continue; thr.add(Integer.parseInt(rec.get("Threads"))); ops.add(Double.parseDouble(rec.get("Score"))); }
        }
        if(thr.isEmpty()) return;
        int w=1000,h=700,l=110,r=50,t=100,b=100; int pw=w-l-r, ph=h-t-b;
        double maxOps=ops.stream().mapToDouble(Double::doubleValue).max().orElse(1.0); double[] ntY=niceTicks(0,maxOps*1.15,6);
        BufferedImage img=new BufferedImage(w,h,BufferedImage.TYPE_INT_RGB); Graphics2D g=img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0,0,w,h); g.setColor(B()); g.setFont(g.getFont().deriveFont(Font.BOLD,22f));
        String title="Indexing — Throughput vs Threads (ops/s)"; g.drawString(title,(w-g.getFontMetrics().stringWidth(title))/2,50);
        yAxis(g,l,t,pw,ph,ntY[0],ntY[1],6,"");
        g.setStroke(new BasicStroke(2.5f)); int px=-1,py=-1;
        for(int i=0;i<thr.size();i++){ int x=l+(int)Math.round((i/(double)(thr.size()-1))*pw); int y=t+(int)Math.round((1.0-(ops.get(i)-ntY[0])/(ntY[1]-ntY[0]))*ph); g.setColor(B()); g.fillOval(x-4,y-4,8,8); if(px>=0) g.drawLine(px,py,x,y); px=x; py=y; g.setFont(g.getFont().deriveFont(14f)); String lbl=thr.get(i)+""; int tw=g.getFontMetrics().stringWidth(lbl); g.setColor(Color.BLACK); g.drawString(lbl,x-tw/2,t+ph+30); }
        xTitle(g,"Threads",l,t,pw,ph); ImageIO.write(img,"png",outPng.toFile()); g.dispose();
    }

    public static void plotLatencyDistAcrossIterations(Path itersCsv, Path outPng) throws Exception {
        if(!Files.exists(itersCsv)) return;
        List<Double> ms=new ArrayList<>();
        try(Reader r=Files.newBufferedReader(itersCsv, StandardCharsets.UTF_8); CSVParser p=CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim().parse(r)){
            for(CSVRecord rec:p){ if(!rec.get("phase").equalsIgnoreCase("iteration")) continue; if(!rec.get("mode").equalsIgnoreCase("avgt")) continue; String unit=rec.get("unit").toLowerCase(Locale.ROOT); double v=Double.parseDouble(rec.get("value")); double valMs = unit.contains("s/op") ? v*1000.0 : unit.contains("ms/op") ? v : unit.contains("us/op") ? v/1000.0 : v; ms.add(valMs); }
        }
        if(ms.isEmpty()) return;
        Collections.sort(ms);
        int w=1100,h=700,l=110,r=50,t=100,b=120; int pw=w-l-r, ph=h-t-b;
        int bins=Math.min(50,Math.max(20,ms.size()/5));
        double max=ms.get(ms.size()-1); int[] cnt=new int[bins];
        for(double v:ms){ int i=(int)Math.floor((v/max)*(bins-1)); if(i<0)i=0; if(i>=bins)i=bins-1; cnt[i]++; }
        int maxC=Arrays.stream(cnt).max().orElse(1);
        BufferedImage img=new BufferedImage(w,h,BufferedImage.TYPE_INT_RGB); Graphics2D g=img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0,0,w,h); g.setColor(B()); g.setFont(g.getFont().deriveFont(Font.BOLD,20f));
        String title="Indexing — Latency Distribution Across Iterations (ms/op)"; g.drawString(title,(w-g.getFontMetrics().stringWidth(title))/2,50);
        yAxis(g,l,t,pw,ph,0,maxC,6,"");
        double[] ntX=niceTicks(0,max,6); g.setFont(g.getFont().deriveFont(13f)); FontMetrics fm=g.getFontMetrics();
        for(double v=ntX[0]; v<=ntX[1]+1e-12; v+=ntX[2]){ int x=l+(int)Math.round(((v-ntX[0])/(ntX[1]-ntX[0]))*pw); g.setColor(new Color(220,220,220)); g.drawLine(x,t,x,t+ph); String lbl=String.format(Locale.ROOT,"%.0f ms",v); int tw=fm.stringWidth(lbl); g.setColor(Color.BLACK); g.drawString(lbl,x-tw/2,t+ph+fm.getAscent()*2+6); }
        int barW=Math.max(1,pw/bins); g.setColor(B());
        for(int i=0;i<bins;i++){ int hBar=(int)Math.round((cnt[i]/(double)maxC)*ph); int x=l+i*barW; int y=t+ph-hBar; g.fillRect(x,y,Math.max(1,barW-2),hBar); }
        xTitle(g,"Latency (ms/op)",l,t,pw,ph); ImageIO.write(img,"png",outPng.toFile()); g.dispose();
    }

    public static void plotCpuOverTime(Path dataDir, Path outPng) throws Exception {
        List<Path> sys = Files.list(dataDir).filter(p -> p.getFileName().toString().startsWith("indexing_sys_t") && p.toString().endsWith(".csv")).sorted().collect(Collectors.toList());
        if(sys.isEmpty()) return;
        List<long[]> seriesT=new ArrayList<>(); List<double[]> seriesV=new ArrayList<>(); long minT=Long.MAX_VALUE,maxT=Long.MIN_VALUE;
        for(Path p:sys){ List<Long> ts=new ArrayList<>(); List<Double> v=new ArrayList<>(); try(Reader r=Files.newBufferedReader(p,StandardCharsets.UTF_8); CSVParser c=CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(r)){ for(CSVRecord rec:c){ long tms=Long.parseLong(rec.get("timestamp_ms")); double cpu=Double.parseDouble(rec.get("cpu_percent")); ts.add(tms); v.add(cpu); minT=Math.min(minT,tms); maxT=Math.max(maxT,tms); } } seriesT.add(ts.stream().mapToLong(x->x).toArray()); seriesV.add(v.stream().mapToDouble(x->x).toArray()); }
        int w=1100,h=700,l=110,r=50,t=100,b=120; int pw=w-l-r, ph=h-t-b;
        BufferedImage img=new BufferedImage(w,h,BufferedImage.TYPE_INT_RGB); Graphics2D g=img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0,0,w,h); g.setColor(B()); g.setFont(g.getFont().deriveFont(Font.BOLD,20f));
        String title="Indexing — CPU Usage Over Time (%)"; g.drawString(title,(w-g.getFontMetrics().stringWidth(title))/2,50);
        yAxis(g,l,t,pw,ph,0,100,6," %");
        double minS=0, maxS=Math.max(1e-9,(maxT-minT)/1000.0); double[] ntX=niceTicks(minS,maxS,6);
        g.setFont(g.getFont().deriveFont(13f)); FontMetrics fm=g.getFontMetrics();
        for(double v=ntX[0]; v<=ntX[1]+1e-12; v+=ntX[2]){ int x=l+(int)Math.round(((v-ntX[0])/(ntX[1]-ntX[0]))*pw); g.setColor(new Color(220,220,220)); g.drawLine(x,t,x,t+ph); String lbl=String.format(Locale.ROOT,"%.0f s",v); int tw=fm.stringWidth(lbl); g.setColor(Color.BLACK); g.drawString(lbl,x-tw/2,t+ph+fm.getAscent()*2+6); }
        g.setStroke(new BasicStroke(2.0f)); g.setColor(Color.BLACK);
        for(int s=0;s<seriesT.size();s++){ long[] ts=seriesT.get(s); double[] vv=seriesV.get(s); int px=-1,py=-1; for(int i=0;i<ts.length;i++){ double xs=(ts[i]-minT)/1000.0; int x=l+(int)Math.round(((xs-ntX[0])/(ntX[1]-ntX[0]))*pw); int y=t+(int)Math.round((1.0-(vv[i]/100.0))*ph); if(px>=0) g.drawLine(px,py,x,y); g.fillOval(x-2,y-2,4,4); px=x; py=y; } }
        xTitle(g,"Time (s)",l,t,pw,ph); ImageIO.write(img,"png",outPng.toFile()); g.dispose();
    }

    public static void plotMemoryOverTime(Path dataDir, Path outPng) throws Exception {
        List<Path> sys = Files.list(dataDir).filter(p -> p.getFileName().toString().startsWith("indexing_sys_t") && p.toString().endsWith(".csv")).sorted().collect(Collectors.toList());
        if(sys.isEmpty()) return;
        List<long[]> seriesT=new ArrayList<>(); List<double[]> seriesU=new ArrayList<>(); long minT=Long.MAX_VALUE,maxT=Long.MIN_VALUE; double maxMb=1.0;
        for(Path p:sys){ List<Long> ts=new ArrayList<>(); List<Double> v=new ArrayList<>(); try(Reader r=Files.newBufferedReader(p,StandardCharsets.UTF_8); CSVParser c=CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(r)){ for(CSVRecord rec:c){ long tms=Long.parseLong(rec.get("timestamp_ms")); double used=Double.parseDouble(rec.get("used_memory_mb")); ts.add(tms); v.add(used); minT=Math.min(minT,tms); maxT=Math.max(maxT,tms); maxMb=Math.max(maxMb,used); } } seriesT.add(ts.stream().mapToLong(x->x).toArray()); seriesU.add(v.stream().mapToDouble(x->x).toArray()); }
        int w=1100,h=700,l=110,r=50,t=100,b=120; int pw=w-l-r, ph=h-t-b;
        double[] ntY=niceTicks(0,maxMb*1.1,6);
        BufferedImage img=new BufferedImage(w,h,BufferedImage.TYPE_INT_RGB); Graphics2D g=img.createGraphics(); aa(g);
        g.setColor(Color.WHITE); g.fillRect(0,0,w,h); g.setColor(B()); g.setFont(g.getFont().deriveFont(Font.BOLD,20f));
        String title="Indexing — Memory Usage Over Time (MB)"; g.drawString(title,(w-g.getFontMetrics().stringWidth(title))/2,50);
        yAxis(g,l,t,pw,ph,ntY[0],ntY[1],6," MB");
        double minS=0, maxS=Math.max(1e-9,(maxT-minT)/1000.0); double[] ntX=niceTicks(minS,maxS,6);
        g.setFont(g.getFont().deriveFont(13f)); FontMetrics fm=g.getFontMetrics();
        for(double v=ntX[0]; v<=ntX[1]+1e-12; v+=ntX[2]){ int x=l+(int)Math.round(((v-ntX[0])/(ntX[1]-ntX[0]))*pw); g.setColor(new Color(220,220,220)); g.drawLine(x,t,x,t+ph); String lbl=String.format(Locale.ROOT,"%.0f s",v); int tw=fm.stringWidth(lbl); g.setColor(Color.BLACK); g.drawString(lbl,x-tw/2,t+ph+fm.getAscent()*2+6); }
        g.setStroke(new BasicStroke(2.0f)); g.setColor(Color.BLACK);
        for(int s=0;s<seriesT.size();s++){ long[] ts=seriesT.get(s); double[] vv=seriesU.get(s); int px=-1,py=-1; for(int i=0;i<ts.length;i++){ double xs=(ts[i]-minT)/1000.0; int x=l+(int)Math.round(((xs-ntX[0])/(ntX[1]-ntX[0]))*pw); int y=t+(int)Math.round((1.0-(vv[i]-ntY[0])/(ntY[1]-ntY[0]))*ph); if(px>=0) g.drawLine(px,py,x,y); g.fillOval(x-2,y-2,4,4); px=x; py=y; } }
        xTitle(g,"Time (s)",l,t,pw,ph); ImageIO.write(img,"png",outPng.toFile()); g.dispose();
    }

    public static void main(String[] args) throws Exception {
        Path base = Paths.get("benchmarking_results/indexing");
        Path data = base.resolve("data");
        Path plots = base.resolve("plots");
        Files.createDirectories(plots);

        plotThroughputVsThreads(data.resolve("indexing_agg.csv"), plots.resolve("indexing_throughput_vs_threads.png"));
        plotLatencyDistAcrossIterations(data.resolve("indexing_data.csv"), plots.resolve("indexing_latency_distribution.png"));
        plotCpuOverTime(data, plots.resolve("indexing_cpu_over_time.png"));
        plotMemoryOverTime(data, plots.resolve("indexing_memory_over_time.png"));

        System.out.println("Indexing plots generated in: " + plots.toAbsolutePath());
    }
}
