package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.regex.Pattern;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfsFSImageToolTest {
    @Test
    public void testVersion() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.main(new String[]{"-V"});

        Pattern pattern = Pattern.compile("Version 1\\..*\n" +
                                "Build timestamp 20.*\n" +
                                "SCM Version .*\n" +
                                "SCM Branch .*\n");
        assertThat(byteArrayOutputStream.toString())
                .matches(pattern);
    }

    @Test
    public void testHelp() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.main(new String[]{"-h"});

        assertThat(byteArrayOutputStream.toString())
                .isEqualTo("Analyze Hadoop FSImage file for user/group reports\n" +
                        "Usage: hfsa-tool [-hV] [-v]... [-fun=<userNameFilter>] [-p=<dirs>[,\n" +
                        "                 <dirs>...]]... FILE [COMMAND]\n" +
                        "      FILE        FSImage file to process.\n" +
                        "      -fun, --filter-by-user=<userNameFilter>\n" +
                        "                  Filter user name by <regexp>.\n" +
                        "  -h, --help      Show this help message and exit.\n" +
                        "  -p, --path=<dirs>[,<dirs>...]\n" +
                        "                  Directory path(s) to start traversing (default: [/]).\n" +
                        "                    Default: [/]\n" +
                        "  -v              Turns on verbose output. Use `-vv` for debug output.\n" +
                        "                    Default: []\n" +
                        "  -V, --version   Print version information and exit.\n" +
                        "Commands:\n" +
                        "  summary\n" +
                        "Runs summary command by default.\n"

                );
    }
}
