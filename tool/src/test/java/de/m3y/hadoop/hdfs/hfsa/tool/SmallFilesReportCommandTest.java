package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class SmallFilesReportCommandTest {
    @Test
    public void testRun() {
        SmallFilesReportCommand command = new SmallFilesReportCommand();
        command.mainCommand = new HdfsFSImageTool.MainCommand();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        command.mainCommand.out = new PrintStream(byteArrayOutputStream);
        command.mainCommand.err = command.mainCommand.out;

        command.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
        command.run();

        assertThat(byteArrayOutputStream.toString())
                .isEqualTo("\n" +
                        "Small files report (< 2 MiB)\n" +
                        "\n" +
                        "Overall small files : 3\n" +
                        "\n" +
                        "Username | Sum small files\n" +
                        "--------------------------\n" +
                        "mm       |               2\n" +
                        "root     |               1\n"
                );
    }

    @Test
    public void testRunWithUserNameFilter() {
        SmallFilesReportCommand command = new SmallFilesReportCommand();
        command.mainCommand = new HdfsFSImageTool.MainCommand();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        command.mainCommand.out = new PrintStream(byteArrayOutputStream);
        command.mainCommand.err = command.mainCommand.out;

        command.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
        command.mainCommand.userNameFilter = "mm";

        command.run();

        assertThat(byteArrayOutputStream.toString())
                .isEqualTo("\n" +
                        "Small files report (< 2 MiB)\n" +
                        "\n" +
                        "Overall small files         : 3\n" +
                        "User (filtered) small files : 2\n" +
                        "\n" +
                        "Username | Sum small files\n" +
                        "--------------------------\n" +
                        "mm       |               2\n"
                );
    }

}
