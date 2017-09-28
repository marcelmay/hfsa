package de.m3y.hadoop.hdfs.hfsa.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IECBinaryTest {
    String[] formattedValues = {"0 B", "1 KiB", "1 KiB", "2 KiB", "3 MiB", "4 GiB", "5 TiB", "6 PiB", "60 PiB"};
    long[] rawValues = {0, 1024, 1024, 2048,
            3 * 1024 * 1024,
            4L * 1024L * 1024L * 1024L,
            5L * 1024L * 1024L * 1024L * 1024L,
            6L * 1024L * 1024L * 1024L * 1024L * 1024L,
            60L * 1024L * 1024L * 1024L * 1024L * 1024L};

    @Test
    public void testParse() {
        for (int i = 0; i < formattedValues.length; i++) {
            assertEquals(rawValues[i], IECBinary.parse(formattedValues[i]));
        }

        assertEquals(0, IECBinary.parse("0"));
        assertEquals(0, IECBinary.parse("0B"));
        assertEquals(1024, IECBinary.parse("1  KiB"));

        // Exceptional values
        for (String value : new String[]{"", " ", "KiB"}) {
            try {
                IECBinary.parse("KiB");
                fail("Expected exception when parsing <" + value + ">");
            } catch (IllegalArgumentException e) {
                // expected
            }
        }
    }

    @Test
    public void testFormat() {
        for (int i = 0; i < formattedValues.length; i++) {
            assertEquals(formattedValues[i], IECBinary.format(rawValues[i]));
        }

        assertEquals("1 KiB", IECBinary.format(1024 + 512 -1));
        assertEquals("2 KiB", IECBinary.format(1024 + 512));
    }
}
