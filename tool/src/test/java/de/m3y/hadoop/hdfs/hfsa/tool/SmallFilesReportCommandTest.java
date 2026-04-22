package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.text.DecimalFormatSymbols;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SmallFilesReportCommandTest {
    static final char DECIMAL_SEPARATOR = DecimalFormatSymbols.getInstance().getDecimalSeparator();

    @Test
    public void testRun() {
        SmallFilesReportCommand command = new SmallFilesReportCommand();
        command.mainCommand = new HdfsFSImageTool.MainCommand();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (PrintStream printStream = new PrintStream(byteArrayOutputStream)) {
            command.mainCommand.out = printStream;
            command.mainCommand.err = command.mainCommand.out;
            command.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
            command.run();
            final String expected = """
                    
                    Small files report (< 2 MiB)
                    
                    Overall small files : 3
                    
                    #Small files  | Path (top 10)\s
                    ------------------------------
                                3 | /
                                2 | /test3
                                1 | /test3/foo
                    
                    Username | #Small files | %
                    ------------------------------------
                    mm       |            2 | 66.7%
                    root     |            1 | 33.3%
                    
                    Username | Small files hotspots (top 10 count/path)
                    ---------------------------------------------------
                    mm       |            2 | /
                             |            1 | /test3
                    ---------------------------------------------------
                    root     |            1 | /
                             |            1 | /test3
                             |            1 | /test3/foo
                    ---------------------------------------------------
                    """;

            assertThat(byteArrayOutputStream)
                    .hasToString(expected.replace('.', DECIMAL_SEPARATOR));
        }
    }

    @Test
    public void testRunWithUserNameFilter() {
        SmallFilesReportCommand command = new SmallFilesReportCommand();
        command.mainCommand = new HdfsFSImageTool.MainCommand();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (PrintStream printStream = new PrintStream(byteArrayOutputStream)) {
            command.mainCommand.out = printStream;
            command.mainCommand.err = command.mainCommand.out;

            command.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
            command.mainCommand.userNameFilter = "mm";

            command.run();

            final String expected = """
                    
                    Small files report (< 2 MiB)
                    
                    Overall small files         : 3
                    User (filtered) small files : 2
                    
                    #Small files  | Path (top 10)\s
                    ------------------------------
                                3 | /
                                2 | /test3
                                1 | /test3/foo
                    
                    Username | #Small files | %
                    ------------------------------------
                    mm       |            2 | 66.7%
                    
                    Username | Small files hotspots (top 10 count/path)
                    ---------------------------------------------------
                    mm       |            2 | /
                             |            1 | /test3
                    ---------------------------------------------------
                    """;
            assertThat(byteArrayOutputStream)
                    .hasToString(expected.replace('.', DECIMAL_SEPARATOR));
        }
    }
}
