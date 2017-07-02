package de.m3y.hadoop.hdfs.hfsa.util;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SizeBucketTest {

    @Test
    public void testBucketAdd() {
        SizeBucket sizeBucket = new SizeBucket();

        assertEquals(0, sizeBucket.findMaxNumBucket());
        sizeBucket.add(0L);
        assertArrayEquals(new long[]{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, sizeBucket.get());
        assertEquals(0, sizeBucket.findMaxNumBucket());
        sizeBucket.add(1L);
        assertArrayEquals(new long[]{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, sizeBucket.get());
        assertEquals(1, sizeBucket.findMaxNumBucket());
        long size = 1024L * 1024L;
        sizeBucket.add(size);
        final long[] expected = {1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        assertArrayEquals(expected, sizeBucket.get());
        for (int i = 0; i < sizeBucket.size() - 3; i++) {
            size *= 2;
            sizeBucket.add(size);
            expected[i + 3]++;
            assertArrayEquals(expected, sizeBucket.get());
            assertEquals(i+3, sizeBucket.findMaxNumBucket());
        }

        sizeBucket.add(0L);
        expected[0]++;
        assertArrayEquals(expected, sizeBucket.get());
        sizeBucket.add(1L);
        expected[1]++;
        assertArrayEquals(expected, sizeBucket.get());
        sizeBucket.add(1024L * 1024L);
        expected[2]++;
        assertArrayEquals(expected, sizeBucket.get());
        sizeBucket.add(2L * 1024L * 1024L);
        expected[3]++;
        assertArrayEquals(expected, sizeBucket.get());
    }
}
