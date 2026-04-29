package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonExportTest {
    @Test
    public void testSummaryJson() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.run(new String[]{"-v", "src/test/resources/fsi_small.img", "summary", "-o", "json"});

        String output = byteArrayOutputStream.toString();
        assertThat(output).contains("\"dirPath\": \"/\"");
        assertThat(output).contains("\"overallStats\": {");
        assertThat(output).contains("\"sumFiles\":");
        assertThat(output).contains("\"sumDirectories\":");
    }

    @Test
    public void testSmallFilesJson() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.run(new String[]{"-v", "src/test/resources/fsi_small.img", "smallfiles", "-o", "json"});

        String output = byteArrayOutputStream.toString();
        assertThat(output).contains("\"sumOverallSmallFiles\":");
        assertThat(output).contains("\"userToReport\": {");
    }

    @Test
    public void testInodeJson() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.run(new String[]{"-v", "src/test/resources/fsi_small.img", "inode", "-o", "json", "16385"});

        String output = byteArrayOutputStream.toString();
        assertThat(output).contains("\"id_\": 16385");
        assertThat(output).contains("\"name_\":");
    }

    @Test
    public void testPathJson() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.run(new String[]{"-v", "src/test/resources/fsi_small.img", "path", "-o", "json"});

        String output = byteArrayOutputStream.toString();
        assertThat(output).contains("\"results\": [");
        assertThat(output).contains("\"fileCount\":");
        assertThat(output).contains("\"dirCount\":");
    }
}
