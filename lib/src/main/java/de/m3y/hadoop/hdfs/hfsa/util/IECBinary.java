package de.m3y.hadoop.hdfs.hfsa.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helps to handle IEC binary units such as KiB.
 */
public class IECBinary {
    private static final Pattern PATTERN_VALUE_WITH_STORAGE_UNIT = Pattern.compile("(\\d+)\\s*(\\w*)");
    private static final String[] UNITS = new String[]{"B", "KiB", "MiB", "GiB", "TiB", "PiB"};

    private IECBinary() {
        // No instantiation
    }

    /**
     * Formats as IEC binary, rounding fractions.
     * <p>
     * Example: 1024 =&gt; 1 KiB
     *
     * @param numericalValue the numerical value
     * @return the formatted value
     */
    public static String format(long numericalValue) {
        // Based on http://programming.guide/java/formatting-byte-size-to-human-readable-format.html
        if (numericalValue < 1024) {
            return numericalValue + " B";
        }
        int exp = (int) (Math.log(numericalValue) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "i";
        return String.format("%.0f %sB", numericalValue / Math.pow(1024, exp), pre);
    }

    /**
     * Parses IEC binary, fraction-less formatted values.
     *
     * @param formattedValue the formatted value like "123 KiB"
     * @return the long value.
     */
    public static long parse(String formattedValue) {
        final Matcher matcher = PATTERN_VALUE_WITH_STORAGE_UNIT.matcher(formattedValue);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Expected pattern " + PATTERN_VALUE_WITH_STORAGE_UNIT.pattern() +
                    " but got value <" + formattedValue + ">");
        }
        long number = Long.parseLong(matcher.group(1));
        String unit = matcher.group(2);
        if (null != unit) {
            for (int i = 0; i < UNITS.length; i++) {
                if (unit.equalsIgnoreCase(UNITS[i])) {
                    number *= Math.pow(1024, i);
                    break;
                }
            }
        }
        return number;
    }
}
