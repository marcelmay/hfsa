package de.m3y.hadoop.hdfs.hfsa.util;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

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
            assertThat(IECBinary.parse(formattedValues[i])).isEqualTo(rawValues[i]);
        }

        assertThat(IECBinary.parse("0")).isZero();
        assertThat(IECBinary.parse("0B")).isZero();
        assertThat(IECBinary.parse("1  KiB")).isEqualTo(1024L);

        // Exceptional values
        for (String value : new String[]{"", " ", "KiB"}) {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> IECBinary.parse(value));
        }
    }

    @Test
    public void testFormat() {
        for (int i = 0; i < formattedValues.length; i++) {
            assertThat(IECBinary.format(rawValues[i])).isEqualTo(formattedValues[i]);
        }

        assertThat(IECBinary.format(1024 + 512 - 1)).isEqualTo("1 KiB");
        assertThat(IECBinary.format(1024 + 512)).isEqualTo("2 KiB");
    }
}
