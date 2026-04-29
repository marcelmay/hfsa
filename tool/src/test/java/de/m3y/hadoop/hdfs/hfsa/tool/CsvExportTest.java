package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvExportTest {
    @Test
    public void testSummaryCsv() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.run(new String[]{"-v", "src/test/resources/fsi_small.img", "summary", "-o", "csv"});

        String output = byteArrayOutputStream.toString();
        assertThat(output).contains("Type,Name,Directories,Symlinks,Files,Size,Blocks,Size Buckets (0B to 256MiB+)");
        assertThat(output).contains("Overall,/,8,0,11,");
        assertThat(output).contains("Group,supergroup,8,0,8,");
        assertThat(output).contains("User,mm,8,0,9,");
    }

    @Test
    public void testSmallFilesCsv() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.run(new String[]{"-v", "src/test/resources/fsi_small.img", "smallfiles", "-o", "csv"});

        String output = byteArrayOutputStream.toString();
        assertThat(output).contains("Type,Name,Path,Small Files");
        assertThat(output).contains("Overall,/,");
        assertThat(output).contains("User,mm,");
        assertThat(output).contains("Hotspot,mm,/,");
    }

    @Test
    public void testPathCsv() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.run(new String[]{"-v", "src/test/resources/fsi_small.img", "path", "-o", "csv"});

        String output = byteArrayOutputStream.toString();
        assertThat(output).contains("Permission,Type,Path");
        assertThat(output).contains(",d,/");
    }

    @Test
    public void testInodeCsv() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.run(new String[]{"-v", "src/test/resources/fsi_small.img", "inode", "-o", "csv", "16385"});

        String output = byteArrayOutputStream.toString();
        assertThat(output).contains("ID,Name,Type");
        assertThat(output).contains("16385,,DIRECTORY");
    }
}
