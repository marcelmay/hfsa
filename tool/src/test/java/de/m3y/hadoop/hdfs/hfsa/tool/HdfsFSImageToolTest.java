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

        HdfsFSImageTool.run(new String[]{"-V"});

        Pattern pattern = Pattern.compile("""
                Version 1\\..*
                Build timestamp 20.*
                SCM Version .*
                SCM Branch .*
                """);
        assertThat(byteArrayOutputStream.toString())
                .matches(pattern);
    }

    @Test
    public void testHelp() {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HdfsFSImageTool.out = new PrintStream(byteArrayOutputStream);
        HdfsFSImageTool.err = HdfsFSImageTool.out;

        HdfsFSImageTool.run(new String[]{"-h"});

        assertThat(byteArrayOutputStream)
                .hasToString("""
                        Analyze Hadoop FSImage file for user/group reports
                        Usage: hfsa-tool [-hVv] [-fun=<userNameFilter>] [-o=<outputFormat>] [-p=<dirs>[,
                                         <dirs>...]]... FILE [COMMAND]
                              FILE        FSImage file to process.
                              -fun, --filter-by-user=<userNameFilter>
                                          Filter user name by <regexp>.
                          -h, --help      Show this help message and exit.
                          -o, --output=<outputFormat>
                                          Enable output format (json, csv or txt). Default is txt.
                                            Default: txt
                          -p, --path=<dirs>[,<dirs>...]
                                          Directory path(s) to start traversing (default: [/]).
                                            Default: [/]
                          -v              Turns on verbose output. Use `-vv` for debug output.
                          -V, --version   Print version information and exit.
                        Commands:
                          summary         Generates an HDFS usage summary (default command if no other
                                            command specified)
                          smallfiles, sf  Reports on small file usage
                          inode, i        Shows INode details
                          path, p         Lists INode paths
                          userusage, uu   Reports on top usage (e.g. size) locations of a user
                        Runs summary command by default.
                        """

                );
    }
}
