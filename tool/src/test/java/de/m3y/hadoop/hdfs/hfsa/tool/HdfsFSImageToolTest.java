package de.m3y.hadoop.hdfs.hfsa.tool;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HdfsFSImageToolTest {
    @Test
    public void testFilter() {
        final CliOptions options = new CliOptions();

        final List<HdfsFSImageTool.UserStats> list = Arrays.asList(new HdfsFSImageTool.UserStats("foobar"),
                new HdfsFSImageTool.UserStats("foo_bar"), new HdfsFSImageTool.UserStats("fo_obar"),
                new HdfsFSImageTool.UserStats("nofoobar"));

        options.userFilter = "^foo.*";
        List<HdfsFSImageTool.UserStats> filtered = HdfsFSImageTool.filter(list, options);
        assertEquals(2, filtered.size());
        assertEquals("foobar", filtered.get(0).userName);
        assertEquals("foo_bar", filtered.get(1).userName);

        options.userFilter = "foo.*";
        filtered = HdfsFSImageTool.filter(list, options);
        assertEquals(3, filtered.size());
        assertEquals("foobar", filtered.get(0).userName);
        assertEquals("foo_bar", filtered.get(1).userName);
        assertEquals("nofoobar", filtered.get(2).userName);

        options.userFilter = ".*bar.*";
        filtered = HdfsFSImageTool.filter(list, options);
        assertEquals(list.size(), filtered.size());
    }
}
