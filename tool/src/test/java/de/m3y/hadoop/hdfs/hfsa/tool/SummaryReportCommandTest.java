package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static de.m3y.hadoop.hdfs.hfsa.tool.SummaryReportCommand.UserStats;
import static de.m3y.hadoop.hdfs.hfsa.tool.SummaryReportCommand.filterByUserName;
import static org.assertj.core.api.Assertions.assertThat;

public class SummaryReportCommandTest {
    @Test
    public void testRun() {
        SummaryReportCommand summaryReportCommand = new SummaryReportCommand();
        summaryReportCommand.mainCommand = new HdfsFSImageTool.MainCommand();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (PrintStream printStream = new PrintStream(byteArrayOutputStream)) {
            summaryReportCommand.mainCommand.out = printStream;
            summaryReportCommand.mainCommand.err = summaryReportCommand.mainCommand.out;

            summaryReportCommand.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
            summaryReportCommand.run();

            assertThat(byteArrayOutputStream)
                    .hasToString("\n" +
                            "HDFS Summary : /\n" +
                            "----------------\n" +
                            "\n" +
                            "#Groups  | #Users      | #Directories | #Symlinks |  #Files     | Size [MB] | CSize[MB] | #Blocks   | File Size Buckets \n" +
                            "         |             |              |           |             |           |           |           | 0 B 1 MiB 2 MiB 4 MiB 8 MiB 16 MiB 32 MiB 64 MiB 128 MiB 256 MiB\n" +
                            "----------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                            "       3 |           3 |            8 |         0 |         11 |       331 |       331 |        12 |   0     2     1     2     1      0      2      1       1       1\n" +
                            "\n" +
                            "By group:            3 | #Directories | #SymLinks | #File      | Size [MB] | CSize[MB] | #Blocks   | File Size Buckets\n" +
                            "                       |              |           |            |           |           |           | 0 B 1 MiB 2 MiB 4 MiB 8 MiB 16 MiB 32 MiB 64 MiB 128 MiB 256 MiB\n" +
                            "---------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                            "                  root |            0 |         0 |          1 |         0 |         0 |         1 |   0     1     0     0     0      0      0      0       0       0\n" +
                            "            supergroup |            8 |         0 |          8 |       151 |       151 |         8 |   0     1     1     2     1      0      1      1       1       0\n" +
                            "                nobody |            0 |         0 |          2 |       180 |       180 |         3 |   0     0     0     0     0      0      1      0       0       1\n" +
                            "\n" +
                            "By user:             3 | #Directories | #SymLinks | #File      | Size [MB] | CSize[MB] | #Blocks   | File Size Buckets\n" +
                            "                       |              |           |            |           |           |           | 0 B 1 MiB 2 MiB 4 MiB 8 MiB 16 MiB 32 MiB 64 MiB 128 MiB 256 MiB\n" +
                            "---------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                            "                  root |            0 |         0 |          1 |         0 |         0 |         1 |   0     1     0     0     0      0      0      0       0       0\n" +
                            "                   foo |            0 |         0 |          1 |       160 |       160 |         2 |   0     0     0     0     0      0      0      0       0       1\n" +
                            "                    mm |            8 |         0 |          9 |       171 |       171 |         9 |   0     1     1     2     1      0      2      1       1       0\n"
                    );
        }
    }

    @Test
    public void testRunWithFilterForUserFoo() {
        SummaryReportCommand summaryReportCommand = new SummaryReportCommand();
        summaryReportCommand.mainCommand = new HdfsFSImageTool.MainCommand();

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (PrintStream printStream = new PrintStream(byteArrayOutputStream)) {
            summaryReportCommand.mainCommand.out = printStream;
            summaryReportCommand.mainCommand.err = printStream;

            summaryReportCommand.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
            summaryReportCommand.mainCommand.userNameFilter = "foo";

            summaryReportCommand.run();

            assertThat(byteArrayOutputStream)
                    .hasToString("\n" +
                            "HDFS Summary : /\n" +
                            "----------------\n" +
                            "\n" +
                            "#Groups  | #Users      | #Directories | #Symlinks |  #Files     | Size [MB] | CSize[MB] | #Blocks   | File Size Buckets \n" +
                            "         |             |              |           |             |           |           |           | 0 B 1 MiB 2 MiB 4 MiB 8 MiB 16 MiB 32 MiB 64 MiB 128 MiB 256 MiB\n" +
                            "----------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                            "       3 |           3 |            8 |         0 |         11 |       331 |       331 |        12 |   0     2     1     2     1      0      2      1       1       1\n" +
                            "\n" +
                            "By group:            3 | #Directories | #SymLinks | #File      | Size [MB] | CSize[MB] | #Blocks   | File Size Buckets\n" +
                            "                       |              |           |            |           |           |           | 0 B 1 MiB 2 MiB 4 MiB 8 MiB 16 MiB 32 MiB 64 MiB 128 MiB 256 MiB\n" +
                            "---------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                            "                  root |            0 |         0 |          1 |         0 |         0 |         1 |   0     1     0     0     0      0      0      0       0       0\n" +
                            "            supergroup |            8 |         0 |          8 |       151 |       151 |         8 |   0     1     1     2     1      0      1      1       1       0\n" +
                            "                nobody |            0 |         0 |          2 |       180 |       180 |         3 |   0     0     0     0     0      0      1      0       0       1\n" +
                            "\n" +
                            "By user:             1 | #Directories | #SymLinks | #File      | Size [MB] | CSize[MB] | #Blocks   | File Size Buckets\n" +
                            "                       |              |           |            |           |           |           | 0 B 1 MiB 2 MiB 4 MiB 8 MiB 16 MiB 32 MiB 64 MiB 128 MiB 256 MiB\n" +
                            "---------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                            "                   foo |            0 |         0 |          1 |       160 |       160 |         2 |   0     0     0     0     0      0      0      0       0       1\n"
                    );
        }
    }

    @Test
    public void testFilter() {
        final List<UserStats> list = Arrays.asList(new UserStats("foobar"),
                new UserStats("foo_bar"), new UserStats("fo_obar"),
                new UserStats("nofoobar"));

        String userNameFilter = "^foo.*";
        List<UserStats> filtered = filterByUserName(list, userNameFilter);
        assertThat(filtered).hasSize(2);
        assertThat(filtered.get(0).userName).isEqualTo("foobar");
        assertThat(filtered.get(1).userName).isEqualTo("foo_bar");

        userNameFilter = "foo.*";
        filtered = filterByUserName(list, userNameFilter);
        assertThat(filtered).extracting(userStats -> userStats.userName)
                .isEqualTo(Arrays.asList("foobar", "foo_bar", "nofoobar"));

        userNameFilter = ".*bar.*";
        filtered = filterByUserName(list, userNameFilter);
        assertThat(filtered).isEqualTo(list);
    }
}
