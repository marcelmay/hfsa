package de.m3y.hadoop.hdfs.hfsa.util;

import java.util.Arrays;

/**
 * Counts size in buckets.
 * <p>
 * Used for getting the size distribution of files.
 */
public class SizeBucket {
    // 0 < 1MB < 2MB < 4MB < ...
    static final int INITIAL_BUCKETS = computeBucket(1024L * 1024L * 1024L * 1024L) + 2;
    private long[] fileSizeBuckets = new long[INITIAL_BUCKETS];

    /**
     * Increments the bucket counter for given file size.
     *
     * @param size the size.
     */
    public void add(long size) {
        int bucket = computeBucket(size);
        if (bucket >= fileSizeBuckets.length) {
            long[] newFileSizeBuckets = new long[bucket];
            System.arraycopy(fileSizeBuckets, 0, newFileSizeBuckets, 0, fileSizeBuckets.length);
            fileSizeBuckets = newFileSizeBuckets;
        }
        fileSizeBuckets[bucket]++;
    }

    /**
     * Computes the bucket for given size.
     * <p>
     * Buckets start at 0B < 1MB < 2MB < 4MB < 8MB ...
     *
     * @param size the size
     * @return the bucket.
     */
    static int computeBucket(long size) {
        if (size == 0L) {
            return 0;
        } else if (size < 1024L * 1024L) {
            return 1;
        }
        if (size < 2L * 1024L * 1024L) {
            return 2;
        }
        double mb = ((double) size) / (double) (2L * 1024L * 1024L);
        final int v = (int) (Math.log(mb) / Math.log(2d));
        return v + 3;
    }

    /**
     * Computes the bucket borders in bytes.
     *
     * @return the bucket sizes.
     */
    long[] computeBucketUpperBorders() {
        long sizes[] = new long[findMaxNumBucket() + 1];
        sizes[0] = 0L;
        sizes[1] = 1024L * 1024; // 1 MiB
        for (int i = 2; i < sizes.length; i++) {
            sizes[i] = sizes[i - 1] * 2L;
        }
        return sizes;
    }

    /**
     * Finds the number of filled buckets.
     *
     * @return the number of filled buckets.
     */
    int findMaxNumBucket() {
        int max = 0;
        for (int i = 0; i < fileSizeBuckets.length; i++) {
            if (fileSizeBuckets[i] > 0) {
                max = i;
            }
        }
        return max;
    }

    long findMaxBucketSizeEntry() {
        long max = 0;
        for (int i = 0; i < fileSizeBuckets.length; i++) {
            final long numBucketEntries = fileSizeBuckets[i];
            if (max < numBucketEntries) {
                max = numBucketEntries;
            }
        }
        return max;

    }

    /**
     * Gets the counter of given bucket.
     *
     * @param bucket index
     * @return the bucket counter.
     */
    long get(int bucket) {
        return fileSizeBuckets[bucket];
    }

    /**
     * Gets all bucket counters.
     *
     * @return array containing bucket counters.
     */
    long[] get() {
        return fileSizeBuckets;
    }

    @Override
    public String toString() {
        return "SizeBucket{" + Arrays.toString(fileSizeBuckets) + "}";
    }
}
