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
                    .isEqualTo("\n" +
                            "Path report (path=/, no filter) :\n" +
                            "---------------------------------\n" +
                            "\n" +
                            "11 files, 8 directories and 0 symlinks\n" +
                            "\n" +
                            "drwxr-xr-x mm   supergroup /\n" +
                            "drwxr-xr-x mm   supergroup /test1\n" +
                            "drwxr-xr-x mm   supergroup /test2\n" +
                            "drwxr-xr-x mm   supergroup /test3\n" +
                            "drwxr-xr-x mm   supergroup /test3/foo\n" +
                            "drwxr-xr-x mm   supergroup /test3/foo/bar\n" +
                            "-rw-r--r-- mm   nobody     /test3/foo/bar/test_20MiB.img\n" +
                            "-rw-r--r-- mm   supergroup /test3/foo/bar/test_2MiB.img\n" +
                            "-rw-r--r-- mm   supergroup /test3/foo/bar/test_40MiB.img\n" +
                            "-rw-r--r-- mm   supergroup /test3/foo/bar/test_4MiB.img\n" +
                            "-rw-r--r-- mm   supergroup /test3/foo/bar/test_5MiB.img\n" +
                            "-rw-r--r-- mm   supergroup /test3/foo/bar/test_80MiB.img\n" +
                            "-rw-r--r-- root root       /test3/foo/test_1KiB.img\n" +
                            "-rw-r--r-- mm   supergroup /test3/foo/test_20MiB.img\n" +
                            "-rw-r--r-- mm   supergroup /test3/test.img\n" +
                            "-rw-r--r-- foo  nobody     /test3/test_160MiB.img\n" +
                            "-rw-r--r-- mm   supergroup /test_2KiB.img\n" +
                            "drwxr-xr-x mm   supergroup /user\n" +
                            "drwxr-xr-x mm   supergroup /user/mm\n"
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

            assertThat(byteArrayOutputStream.toString())
                    .isEqualTo("\n" +
                            "Path report (path=/, user=~foo) :\n" +
                            "---------------------------------\n" +
                            "\n" +
                            "1 file, 0 directories and 0 symlinks\n" +
                            "\n" +
                            "-rw-r--r-- foo nobody /test3/test_160MiB.img\n"
                    );
        }
    }

}
