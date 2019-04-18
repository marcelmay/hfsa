package de.m3y.hadoop.hdfs.hfsa.tool;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static de.m3y.hadoop.hdfs.hfsa.tool.SummaryReport.*;
import static org.assertj.core.api.Assertions.assertThat;

public class HdfsFSImageToolTest {
    @Test
    public void testFilter() {
        final List<UserStats> list = Arrays.asList(new UserStats("foobar"),
                new UserStats("foo_bar"), new UserStats("fo_obar"),
                new UserStats("nofoobar"));

        String userNameFilter = "^foo.*";
        List<UserStats> filtered = filterByUserName(list, userNameFilter);
        assertThat(filtered.size()).isEqualTo(2);
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
