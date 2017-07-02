package de.m3y.hadoop.hdfs.hfsa.tool;

import de.m3y.hadoop.hdfs.hfsa.util.SizeBucket;

/**
 * Helps formatting output.
 */
public class FormatUtil {
    final static String[] UNITS = new String[] { "B", "KiB", "MiB", "GiB", "TiB" };

    private FormatUtil() {
        // No instantiation
    }

    static String toString(SizeBucket fileSizeBuckets, int maxDigits) {
        final int maxBucket = fileSizeBuckets.findMaxNumBucket();
        StringBuilder buf = new StringBuilder();
        final String format = "%" + maxDigits + "d ";
        for(int i = 0; i<= maxBucket;i++) {
            buf.append(String.format(format, fileSizeBuckets.get(i)));
        }
        return buf.toString();
    }

    static String toStorageUnit(long size) {
        if(0L == size) {
            return "0 B";
        }
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return (int)(size/Math.pow(1024, digitGroups)) + " " + UNITS[digitGroups];
    }

    static String[] toStringSizeFormatted(long[] size) {
        String[] units = new String[size.length];
        for(int i=0;i<units.length;i++) {
            units[i] = toStorageUnit(size[i]);
        }
        return units;
    }

    static String formatForLengths(int[] lengths, String formatType) {
        StringBuilder buf = new StringBuilder();
        for(int i=0;i<lengths.length;i++) {
            if(i>0) {
                buf.append(' ');
            }
            buf.append('%').append(lengths[i]).append(formatType);
        }
        return buf.toString();
    }

    static int numberOfDigits(long l) {
        return (int) Math.ceil(Math.log(l) / Math.log(10d));
    }

    static int[] numberOfDigits(long numbers[]) {
        int[] lenghts = new int[numbers.length];
        for(int i=0;i<lenghts.length;i++) {
            lenghts[i] = numberOfDigits(numbers[i]);
        }
        return  lenghts;
    }

    static int[] length(String[] bucketUnits) {
        int len[] = new int[bucketUnits.length];
        for(int i=0;i<bucketUnits.length;i++) {
            len[i] = bucketUnits[i].length();
        }
        return len;
    }

    static int[] max(int[] a, int[] b) {
        int[] m = new int[a.length];
        for(int i=0;i<a.length;i++) {
            m[i] = Math.max(a[i],b[i]);
        }
        return m;
    }

    static String padRight(char s, int length) {
        StringBuilder buf = new StringBuilder();
        while(buf.length()<length) {
            buf.append(s);
        }
        return buf.toString();
    }
}
