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
            final String expected = "\n" +
                    "Small files report (< 2 MiB)\n" +
                    "\n" +
                    "Overall small files : 3\n" +
                    "\n" +
                    "#Small files  | Path (top 10) \n" +
                    "------------------------------\n" +
                    "            3 | /\n" +
                    "            2 | /test3\n" +
                    "            1 | /test3/foo\n" +
                    "\n" +
                    "Username | #Small files | %\n" +
                    "------------------------------------\n" +
                    "mm       |            2 | 66.7%\n" +
                    "root     |            1 | 33.3%\n" +
                    "\n" +
                    "Username | Small files hotspots (top 10 count/path)\n" +
                    "---------------------------------------------------\n" +
                    "mm       |            2 | /\n" +
                    "         |            1 | /test3\n" +
                    "---------------------------------------------------\n" +
                    "root     |            1 | /\n" +
                    "         |            1 | /test3\n" +
                    "         |            1 | /test3/foo\n" +
                    "---------------------------------------------------\n";

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

            final String expected = "\n" +
                    "Small files report (< 2 MiB)\n" +
                    "\n" +
                    "Overall small files         : 3\n" +
                    "User (filtered) small files : 2\n" +
                    "\n" +
                    "#Small files  | Path (top 10) \n" +
                    "------------------------------\n" +
                    "            3 | /\n" +
                    "            2 | /test3\n" +
                    "            1 | /test3/foo\n" +
                    "\n" +
                    "Username | #Small files | %\n" +
                    "------------------------------------\n" +
                    "mm       |            2 | 66.7%\n" +
                    "\n" +
                    "Username | Small files hotspots (top 10 count/path)\n" +
                    "---------------------------------------------------\n" +
                    "mm       |            2 | /\n" +
                    "         |            1 | /test3\n" +
                    "---------------------------------------------------\n";
            assertThat(byteArrayOutputStream)
                    .hasToString(expected.replace('.', DECIMAL_SEPARATOR) );
        }
    }
}
