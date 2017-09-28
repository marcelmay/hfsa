package de.m3y.hadoop.hdfs.hfsa.tool;

import java.util.Arrays;

import de.m3y.hadoop.hdfs.hfsa.util.IECBinary;

/**
 * Helps formatting output.
 */
class FormatUtil {

    private FormatUtil() {
        // No instantiation
    }

    static String[] toStringSizeFormatted(long[] size) {
        String[] units = new String[size.length];
        for (int i = 0; i < units.length; i++) {
            units[i] = IECBinary.format(size[i]);
        }
        return units;
    }

    static String formatForLengths(int[] lengths, String formatType) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < lengths.length; i++) {
            if (i > 0) {
                buf.append(' ');
            }
            buf.append('%').append(lengths[i]).append(formatType);
        }
        return buf.toString();
    }

    static int numberOfDigits(long l) {
        return (int) Math.ceil(Math.log(l) / Math.log(10d));
    }

    static int[] numberOfDigits(long[] numbers) {
        int[] lenghts = new int[numbers.length];
        for (int i = 0; i < lenghts.length; i++) {
            lenghts[i] = numberOfDigits(numbers[i]);
        }
        return lenghts;
    }

    static int[] length(String[] bucketUnits) {
        int[] len = new int[bucketUnits.length];
        for (int i = 0; i < bucketUnits.length; i++) {
            len[i] = bucketUnits[i].length();
        }
        return len;
    }

    static int[] max(int[] a, int[] b) {
        int[] m = new int[a.length];
        for (int i = 0; i < a.length; i++) {
            m[i] = Math.max(a[i], b[i]);
        }
        return m;
    }

    static String padRight(char c, int length) {
        StringBuilder buf = new StringBuilder();
        while (buf.length() < length) {
            buf.append(c);
        }
        return buf.toString();
    }

    static Object[] boxAndPadWithZeros(int length, long[] values) {
        long[] padded;
        if (values.length == length) {
            padded = values;
        } else {
            padded = new long[length];
            System.arraycopy(values, 0, padded, 0, values.length);
        }
        return Arrays.stream(padded).boxed().toArray();
    }
}
