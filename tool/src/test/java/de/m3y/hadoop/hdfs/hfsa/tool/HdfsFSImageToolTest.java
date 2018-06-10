package de.m3y.hadoop.hdfs.hfsa.tool;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfsFSImageToolTest {
    @Test
    public void testFilter() {
        final CliOptions options = new CliOptions();

        final List<HdfsFSImageTool.UserStats> list = Arrays.asList(new HdfsFSImageTool.UserStats("foobar"),
                new HdfsFSImageTool.UserStats("foo_bar"), new HdfsFSImageTool.UserStats("fo_obar"),
                new HdfsFSImageTool.UserStats("nofoobar"));

        options.userNameFilter = "^foo.*";
        List<HdfsFSImageTool.UserStats> filtered = HdfsFSImageTool.filter(list, options);
        assertThat(filtered.size()).isEqualTo(2);
        assertThat(filtered.get(0).userName).isEqualTo("foobar");
        assertThat(filtered.get(1).userName).isEqualTo("foo_bar");

        options.userNameFilter = "foo.*";
        filtered = HdfsFSImageTool.filter(list, options);
        assertThat(filtered).extracting(userStats -> userStats.userName)
                .isEqualTo(Arrays.asList("foobar", "foo_bar", "nofoobar"));

        options.userNameFilter = ".*bar.*";
        filtered = HdfsFSImageTool.filter(list, options);
        assertThat(filtered).isEqualTo(list);
    }
}
