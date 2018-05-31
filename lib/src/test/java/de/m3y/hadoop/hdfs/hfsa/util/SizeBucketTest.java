package de.m3y.hadoop.hdfs.hfsa.util;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SizeBucketTest {

    @Test
    public void testBucketAdd() {
        SizeBucket sizeBucket = new SizeBucket();

        assertEquals(0, sizeBucket.findMaxNumBucket());
        assertEquals(0, sizeBucket.getBucketCounter(0));
        assertEquals(0, sizeBucket.findMaxBucketCount());
        sizeBucket.add(0L);
        assertArrayEquals(new long[]{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, sizeBucket.get());
        assertEquals(0, sizeBucket.findMaxNumBucket());
        assertEquals(1, sizeBucket.getBucketCounter(0));
        sizeBucket.add(1L);
        assertArrayEquals(new long[]{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, sizeBucket.get());
        assertEquals(1, sizeBucket.findMaxNumBucket());
        assertEquals(1, sizeBucket.findMaxBucketCount());
        long size = 1024L * 1024L;
        sizeBucket.add(size);
        final long[] expected = {1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        assertArrayEquals(expected, sizeBucket.get());
        for (int i = 0; i < sizeBucket.size() - 3; i++) {
            size *= 2;
            sizeBucket.add(size);
            expected[i + 3]++;
            assertArrayEquals(expected, sizeBucket.get());
            assertEquals(i + 3, sizeBucket.findMaxNumBucket());
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

        // Trigger resize
        sizeBucket.add(300L * 1024L * 1024L * 1024L);
        assertArrayEquals(new long[]{
                2, 2, 2, 2, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1}, sizeBucket.get());
    }

    @Test
    public void testMaxBucketCount() {
        SizeBucket sizeBucket = new SizeBucket();
        assertEquals(0, sizeBucket.findMaxBucketCount());

        sizeBucket.add(0L);
        assertEquals(1, sizeBucket.findMaxBucketCount());
        sizeBucket.add(0L);
        assertEquals(2, sizeBucket.findMaxBucketCount());
        sizeBucket.add(2L * 1024L * 1024L);
        sizeBucket.add(2L * 1024L * 1024L);
        sizeBucket.add(2L * 1024L * 1024L);
        sizeBucket.add(2L * 1024L * 1024L);
        sizeBucket.add(2L * 1024L * 1024L);
        assertEquals(5, sizeBucket.findMaxBucketCount());
        sizeBucket.add(100L * 1024L * 1024L);
        sizeBucket.add(100L * 1024L * 1024L);
        assertEquals(5, sizeBucket.findMaxBucketCount());
    }

    @Test
    public void testComputeBucketUpperBorders() {
        SizeBucket sizeBucket = new SizeBucket();
        assertArrayEquals(new long[]{0 /* Default when empty*/}, sizeBucket.computeBucketUpperBorders());
        sizeBucket.add( 1024L);
        assertArrayEquals(new long[]{0 , 1024L * 1024L /* 1 MiB */}, sizeBucket.computeBucketUpperBorders());
    }
}
