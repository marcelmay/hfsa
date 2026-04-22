package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PathReportCommandTest {
    @Test
    public void testRun() {
        PathReportCommand pathReportCommand = new PathReportCommand();
        pathReportCommand.mainCommand = new HdfsFSImageTool.MainCommand();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (PrintStream printStream = new PrintStream(byteArrayOutputStream)) {
            pathReportCommand.mainCommand.out = printStream;
            pathReportCommand.mainCommand.err = pathReportCommand.mainCommand.out;

            pathReportCommand.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
            pathReportCommand.run();

            final String actualStdout = byteArrayOutputStream.toString();
            assertThat(actualStdout)
                    .isEqualTo("""
                            
                            Path report (path=/, no filter) :
                            ---------------------------------
                            
                            11 files, 8 directories and 0 symlinks
                            
                            drwxr-xr-x mm   supergroup /
                            drwxr-xr-x mm   supergroup /test1
                            drwxr-xr-x mm   supergroup /test2
                            drwxr-xr-x mm   supergroup /test3
                            drwxr-xr-x mm   supergroup /test3/foo
                            drwxr-xr-x mm   supergroup /test3/foo/bar
                            -rw-r--r-- mm   nobody     /test3/foo/bar/test_20MiB.img
                            -rw-r--r-- mm   supergroup /test3/foo/bar/test_2MiB.img
                            -rw-r--r-- mm   supergroup /test3/foo/bar/test_40MiB.img
                            -rw-r--r-- mm   supergroup /test3/foo/bar/test_4MiB.img
                            -rw-r--r-- mm   supergroup /test3/foo/bar/test_5MiB.img
                            -rw-r--r-- mm   supergroup /test3/foo/bar/test_80MiB.img
                            -rw-r--r-- root root       /test3/foo/test_1KiB.img
                            -rw-r--r-- mm   supergroup /test3/foo/test_20MiB.img
                            -rw-r--r-- mm   supergroup /test3/test.img
                            -rw-r--r-- foo  nobody     /test3/test_160MiB.img
                            -rw-r--r-- mm   supergroup /test_2KiB.img
                            drwxr-xr-x mm   supergroup /user
                            drwxr-xr-x mm   supergroup /user/mm
                            """
                    );
        }
    }

    @Test
    public void testRunWithFilterForUserFoo() {
        PathReportCommand pathReportCommand = new PathReportCommand();
        pathReportCommand.mainCommand = new HdfsFSImageTool.MainCommand();

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (PrintStream printStream = new PrintStream(byteArrayOutputStream)) {
            pathReportCommand.mainCommand.out = printStream;
            pathReportCommand.mainCommand.err = printStream;

            pathReportCommand.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
            pathReportCommand.mainCommand.userNameFilter = "foo";

            pathReportCommand.run();

            assertThat(byteArrayOutputStream)
                    .hasToString("""
                            
                            Path report (path=/, user=~foo) :
                            ---------------------------------
                            
                            1 file, 0 directories and 0 symlinks
                            
                            -rw-r--r-- foo nobody /test3/test_160MiB.img
                            """
                    );
        }
    }

}
