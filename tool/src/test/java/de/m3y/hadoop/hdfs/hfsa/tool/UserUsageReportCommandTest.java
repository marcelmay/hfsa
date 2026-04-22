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
            final String expected = """
                    
                    Size report (user=mm, start dir=/)
                    
                    /              | 172 MiB
                    /test3         | 172 MiB
                    /test3/foo     | 171 MiB
                    /test3/foo/bar | 151 MiB
                    """;

            assertThat(byteArrayOutputStream)
                    .hasToString(expected.replace('.', DECIMAL_SEPARATOR));
        }
    }

    @Test
    public void testRunWithSubDir() {
        UserUsageReportCommand command = new UserUsageReportCommand();
        command.mainCommand = new HdfsFSImageTool.MainCommand();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (PrintStream printStream = new PrintStream(byteArrayOutputStream)) {
            command.mainCommand.out = printStream;
            command.mainCommand.err = command.mainCommand.out;
            command.mainCommand.dirs = new String[]{"/test3/foo"};
            command.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
            command.user = "mm";
            command.run();
            final String expected = """
                    
                    Size report (user=mm, start dir=/test3/foo)
                    
                    /              | 171 MiB
                    /test3         | 171 MiB
                    /test3/foo     | 171 MiB
                    /test3/foo/bar | 151 MiB
                    """;

            assertThat(byteArrayOutputStream)
                    .hasToString(expected.replace('.', DECIMAL_SEPARATOR));
        }
    }
}
