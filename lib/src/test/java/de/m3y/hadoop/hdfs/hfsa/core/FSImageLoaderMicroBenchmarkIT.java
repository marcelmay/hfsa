package de.m3y.hadoop.hdfs.hfsa.core;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class FSImageLoaderMicroBenchmarkIT {

    static RandomAccessFile openFile() throws FileNotFoundException {
        return new RandomAccessFile("src/test/resources/fsimage_d800_f210k.img", "r");
    }

    @State(Scope.Benchmark)
    public static class LoaderState {
        FsImageLoader imageLoader = new FsImageLoader.Builder().build();
        FsImageLoader parallelImageLoader = new FsImageLoader.Builder().parallel().build();
        FsVisitor.Builder visitorBuilder = new FsVisitor.Builder();
        FsVisitor.Builder parallelVisitorBuilder = new FsVisitor.Builder().parallel();

        FsImageData fsImageData;

        @Setup(Level.Trial)
        public void setUp() {
            try (RandomAccessFile file = openFile()) {
                fsImageData = new FsImageLoader.Builder().build().load(file);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Benchmark
    public void loadFsImageFileParallel(LoaderState state, Blackhole blackhole) throws IOException {
        try (RandomAccessFile file = openFile()) {
            blackhole.consume(state.parallelImageLoader.load(file));
        }
    }

    @Benchmark
    public void loadFsImageFile(LoaderState state, Blackhole blackhole) throws IOException {
        try (RandomAccessFile file = openFile()) {
            blackhole.consume(state.imageLoader.load(file));
        }
    }

    @Benchmark
    public void visitFsImageFile(LoaderState state, Blackhole blackhole) throws IOException {
        state.visitorBuilder.visit(state.fsImageData, new BenchmarkVisitor(blackhole));
    }

    @Benchmark
    public void visitParallelFsImageFile(LoaderState state, Blackhole blackhole) throws IOException {
        state.parallelVisitorBuilder.visit(state.fsImageData, new BenchmarkVisitor(blackhole));
    }

    @Test
    public void runMicroBenchMark() throws RunnerException {
        String reportPath = "target/jmh-reports/";
        new File(reportPath).mkdirs();
        Options opt = new OptionsBuilder()
                .include(getClass().getName())
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .addProfiler(GCProfiler.class)
                .jvmArgs("-server", "-XX:+UseG1GC", "-Xmx2048m", "-Dlog4j.configuration=log4j-it.xml")
                .shouldDoGC(true)
                .resultFormat(ResultFormatType.JSON)
                .result(reportPath + getClass().getSimpleName() + ".json")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    private static class BenchmarkVisitor implements FsVisitor {
        private final Blackhole blackhole;

        public BenchmarkVisitor(Blackhole blackhole) {
            this.blackhole = blackhole;
        }

        @Override
        public void onFile(FsImageFile fsImageFile) {
            blackhole.consume(fsImageFile.getInode());
        }

        @Override
        public void onDirectory(FsImageDir fsImageDir) {
            blackhole.consume(fsImageDir.getInode());
        }

        @Override
        public void onSymLink(FsImageSymLink fsImageSymLink) {
            blackhole.consume(fsImageSymLink.getInode());
        }
    }

    public static void main(String[] args) throws RunnerException {
        new FSImageLoaderMicroBenchmarkIT().runMicroBenchMark();
    }
}
