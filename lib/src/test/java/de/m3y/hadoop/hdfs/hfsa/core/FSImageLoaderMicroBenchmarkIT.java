package de.m3y.hadoop.hdfs.hfsa.core;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
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
            FSImageLoader loader;

           @Setup(Level.Trial)
           public void setUp() {
               try {
                   loader = FSImageLoader.load(openFile());
               } catch (IOException e) {
                   throw new IllegalStateException(e);
               }
           }
       }

    @Benchmark
    public void loadFsImageFile(Blackhole blackhole) throws IOException {
        blackhole.consume(FSImageLoader.load(openFile()));
    }

    @Benchmark
    public void visitFsImageFile(LoaderState state, Blackhole blackhole) throws IOException {
        state.loader.visit(new BenchmarkVisitor(blackhole));
    }

    @Benchmark
    public void visitParallelFsImageFile(LoaderState state, Blackhole blackhole) throws IOException {
        state.loader.visitParallel(new BenchmarkVisitor(blackhole));
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
                .jvmArgs("-server", "-XX:+UseG1GC", "-Xmx2048m","-Dlog4j.configuration=log4j-it.xml")
                .shouldDoGC(true)
                .resultFormat(ResultFormatType.JSON)
                .result(reportPath+getClass().getSimpleName()+".json")
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
        public void onFile(FsImageProto.INodeSection.INode inode, String path) {
            blackhole.consume(inode);
        }

        @Override
        public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
            blackhole.consume(inode);
        }

        @Override
        public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
            blackhole.consume(inode);
        }
    }
}
