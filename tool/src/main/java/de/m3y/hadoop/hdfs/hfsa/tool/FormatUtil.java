package de.m3y.hadoop.hdfs.hfsa.tool;

import java.util.Arrays;

import de.m3y.hadoop.hdfs.hfsa.util.IECBinary;

/**
 * Helps to format output.
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

    static String numberOfDigitsFormat(long value) {
        int l = numberOfDigits(value);
        return "%"+l+"."+l+"s";
    }

    static int numberOfDigits(long l) {
        if (l == 0) {
            return 1;
        }
        return ((int) Math.ceil(Math.log10(l + 0.5)));
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

    /**
     * Appends chars to string buffer.
     * @param buf the string buffer
     * @param c the char
     * @param count the number of chars to append
     */
    public static void padRight(StringBuilder buf, char c, int count) {
        for(int i=0;i<count;i++) {
            buf.append(c);
        }
    }

    /**
     * Pad and box primitive array.
     *
     * @param length the max length.
     * @param values the values.
     * @return a new array, with boxed and zero-padded values.
     */
    static Object[] boxAndPadWithZeros(int length, long[] values) {
        long[] padded;
        if (values.length == length) {
            padded = values;
        } else {
            padded = new long[length];
            int len = Math.min(padded.length, values.length);
            System.arraycopy(values, 0, padded, 0, len);
        }
        return Arrays.stream(padded).boxed().toArray();
    }
}
