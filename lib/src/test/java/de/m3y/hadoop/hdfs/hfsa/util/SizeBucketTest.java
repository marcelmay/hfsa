package de.m3y.hadoop.hdfs.hfsa.util;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SizeBucketTest {

    @Test
    public void testBucketAdd() {
        SizeBucket sizeBucket = new SizeBucket();

        assertThat(sizeBucket.findMaxNumBucket()).isEqualTo(0);
        assertThat(sizeBucket.getBucketCounter(0)).isEqualTo(0);
        assertThat(sizeBucket.findMaxBucketCount()).isEqualTo(0);
        sizeBucket.add(0L);
        assertThat(sizeBucket.get()).isEqualTo(new long[]{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        assertThat(sizeBucket.findMaxNumBucket()).isEqualTo(0);
        assertThat(sizeBucket.getBucketCounter(0)).isEqualTo(1);
        sizeBucket.add(1L);
        assertThat(sizeBucket.get()).isEqualTo(new long[]{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        assertThat(sizeBucket.findMaxNumBucket()).isEqualTo(1);
        assertThat(sizeBucket.findMaxBucketCount()).isEqualTo(1);
        long size = 1024L * 1024L;
        sizeBucket.add(size);
        final long[] expected = {1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        assertThat(sizeBucket.get()).isEqualTo(expected);
        for (int i = 0; i < sizeBucket.size() - 3; i++) {
            size *= 2;
            sizeBucket.add(size);
            expected[i + 3]++;
            assertThat(sizeBucket.get()).isEqualTo(expected);
            assertThat(sizeBucket.findMaxNumBucket()).isEqualTo(i + 3);
        }

        sizeBucket.add(0L);
        expected[0]++;
        assertThat(sizeBucket.get()).isEqualTo(expected);
        sizeBucket.add(1L);
        expected[1]++;
        assertThat(sizeBucket.get()).isEqualTo(expected);
        sizeBucket.add(1024L * 1024L);
        expected[2]++;
        assertThat(sizeBucket.get()).isEqualTo(expected);
        sizeBucket.add(2L * 1024L * 1024L);
        expected[3]++;
        assertThat(sizeBucket.get()).isEqualTo(expected);

        // Trigger resize
        sizeBucket.add(300L * 1024L * 1024L * 1024L);
        assertThat(sizeBucket.get()).isEqualTo(new long[]{
                2, 2, 2, 2, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1});
    }

    @Test
    public void testMaxBucketCount() {
        SizeBucket sizeBucket = new SizeBucket();
        assertThat(sizeBucket.findMaxBucketCount()).isEqualTo(0);

        sizeBucket.add(0L);
        assertThat(sizeBucket.findMaxBucketCount()).isEqualTo(1);
        sizeBucket.add(0L);
        assertThat(sizeBucket.findMaxBucketCount()).isEqualTo(2);
        sizeBucket.add(2L * 1024L * 1024L);
        sizeBucket.add(2L * 1024L * 1024L);
        sizeBucket.add(2L * 1024L * 1024L);
        sizeBucket.add(2L * 1024L * 1024L);
        sizeBucket.add(2L * 1024L * 1024L);
        assertThat(sizeBucket.findMaxBucketCount()).isEqualTo(5);
        sizeBucket.add(100L * 1024L * 1024L);
        sizeBucket.add(100L * 1024L * 1024L);
        assertThat(sizeBucket.findMaxBucketCount()).isEqualTo(5);
    }

    @Test
    public void testComputeBucketUpperBorders() {
        SizeBucket sizeBucket = new SizeBucket();
        assertThat(sizeBucket.computeBucketUpperBorders()).isEqualTo(new long[]{0 /* Default when empty*/});
        sizeBucket.add(1024L);
        assertThat(sizeBucket.computeBucketUpperBorders()).isEqualTo(new long[]{0, 1024L * 1024L /* 1 MiB */});
    }
}
