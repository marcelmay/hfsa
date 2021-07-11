package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.text.DecimalFormatSymbols;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class UserUsageReportCommandTest {
    static final char DECIMAL_SEPARATOR = DecimalFormatSymbols.getInstance().getDecimalSeparator();

    @Test
    public void testRun() {
        UserUsageReportCommand command = new UserUsageReportCommand();
        command.mainCommand = new HdfsFSImageTool.MainCommand();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (PrintStream printStream = new PrintStream(byteArrayOutputStream)) {
            command.mainCommand.out = printStream;
            command.mainCommand.err = command.mainCommand.out;
            command.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
            command.user = "mm";
            command.run();
            final String expected = "\n" +
                    "Size report (user=mm, start dir=/)\n" +
                    "\n" +
                    "/              | 172 MiB\n" +
                    "/test3         | 172 MiB\n" +
                    "/test3/foo     | 171 MiB\n" +
                    "/test3/foo/bar | 151 MiB\n";

            assertThat(byteArrayOutputStream.toString())
                    .isEqualTo(expected.replace('.', DECIMAL_SEPARATOR));
        }
    }

    @Test
     public void testRunWithSubdir() {
         UserUsageReportCommand command = new UserUsageReportCommand();
         command.mainCommand = new HdfsFSImageTool.MainCommand();
         final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         try (PrintStream printStream = new PrintStream(byteArrayOutputStream)) {
             command.mainCommand.out = printStream;
             command.mainCommand.err = command.mainCommand.out;
             command.mainCommand.dirs = new String[] {"/test3/foo"};
             command.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
             command.user = "mm";
             command.run();
             final String expected = "\n" +
                     "Size report (user=mm, start dir=/test3/foo)\n" +
                     "\n" +
                     "/              | 171 MiB\n" +
                     "/test3         | 171 MiB\n" +
                     "/test3/foo     | 171 MiB\n" +
                     "/test3/foo/bar | 151 MiB\n";

             assertThat(byteArrayOutputStream.toString())
                     .isEqualTo(expected.replace('.', DECIMAL_SEPARATOR));
         }
     }
}
