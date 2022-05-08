package de.m3y.hadoop.hdfs.hfsa.util;

import java.util.Arrays;

/**
 * Counts size in buckets.
 * <p>
 * Used for getting the size distribution of files.
 */
public class SizeBucket {
    private long[] fileSizeBuckets;

    /**
     * Allows different kind of buckets, e.g. fixed size or exponential ones.
     */
    public interface BucketModel {
        /**
         * Computes the bucket index for given value.
         *
         * @param size the value.
         * @return the bucket for this value.
         */
        int computeBucket(long size);

        /**
         * Computes the bucket upper borders.
         * Useful for printing out the buckets.
         *
         * @param maxNumBuckets the maximum number of buckets.
         * @return the bucket upper borders.
         */
        long[] computeBucketUpperBorders(int maxNumBuckets);

        /**
         * Gets the initial number of buckets.
         *
         * @return the number of buckets.
         */
        int getInitialNumberOfBuckets();
    }

    /**
     * Bucket model with computed limits, starting at 0B < 1MB < 2MB < 4MB < 8MB (always doubling).
     */
    static class Bucket2nModel implements BucketModel {
        /**
         * Computes the bucket for given size.
         * <p>
         *
         * @param size the size
         * @return the bucket.
         */
        @Override
        public int computeBucket(long size) {
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
        @Override
        public long[] computeBucketUpperBorders(int maxNumBuckets) {
            long[] sizes = new long[maxNumBuckets + 1];
            sizes[0] = 0L;           // 0 B
            if (sizes.length > 1) {
                sizes[1] = 1024L * 1024L; // 1 MiB
                for (int i = 2; i < sizes.length; i++) {
                    sizes[i] = sizes[i - 1] * 2L;  // doubled
                }
            }
            return sizes;
        }

        @Override
        public int getInitialNumberOfBuckets() {
            return computeBucket(1024L * 1024L * 1024L * 100L /* 100GiB */);
        }
    }

    private final BucketModel bucketModel;

    public SizeBucket() {
        this(new Bucket2nModel());
    }

    public SizeBucket(BucketModel bucketModel) {
        this.bucketModel = bucketModel;
        fileSizeBuckets = new long[bucketModel.getInitialNumberOfBuckets()];
    }

    /**
     * Increments the bucket counter for given file size.
     *
     * @param size the size.
     */
    public void add(long size) {
        int bucket = bucketModel.computeBucket(size);
        if (bucket >= fileSizeBuckets.length) {
            long[] newFileSizeBuckets = new long[bucket + 1];
            System.arraycopy(fileSizeBuckets, 0, newFileSizeBuckets, 0, fileSizeBuckets.length);
            fileSizeBuckets = newFileSizeBuckets;
        }
        fileSizeBuckets[bucket]++;
    }


    /**
     * Computes the bucket upper borders, for the max number filled of buckets.
     *
     * @return the upper bucket borders.
     */
    public long[] computeBucketUpperBorders() {
        return getBucketModel().computeBucketUpperBorders(findMaxNumBucket());
    }

    /**
     * Finds the number of filled buckets.
     *
     * @return the number of filled buckets.
     */
    public int findMaxNumBucket() {
        int max = 0;
        for (int i = 0; i < fileSizeBuckets.length; i++) {
            if (fileSizeBuckets[i] > 0) {
                max = i;
            }
        }
        return max;
    }

    /**
     * Finds the max bucket count.
     *
     * @return the max bucket count.
     */
    long findMaxBucketCount() {
        long max = 0;
        for (final long numBucketEntries : fileSizeBuckets) {
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
    public long getBucketCounter(int bucket) {
        return fileSizeBuckets[bucket];
    }

    /**
     * Gets all bucket counters.
     *
     * @return array containing bucket counters.
     */
    public long[] get() {
        return fileSizeBuckets;
    }

    /**
     * Gets the number of buckets.
     *
     * @return number of buckets.
     */
    public int size() {
        return fileSizeBuckets.length;
    }

    /**
     * Gets the bucket model.
     *
     * @return the model.
     */
    public BucketModel getBucketModel() {
        return bucketModel;
    }

    @Override
    public String toString() {
        return "SizeBucket{" + Arrays.toString(fileSizeBuckets) + "}";
    }
}
