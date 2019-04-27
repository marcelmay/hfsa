package de.m3y.hadoop.hdfs.hfsa.tool;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FormatUtilTest {
    @Test
    public void testNumberOfDigits() {
        assertThat(FormatUtil.numberOfDigits(0)).isEqualTo(1);
        assertThat(FormatUtil.numberOfDigits(1)).isEqualTo(1);
        assertThat(FormatUtil.numberOfDigits(5)).isEqualTo(1);
        assertThat(FormatUtil.numberOfDigits(10)).isEqualTo(2);
        assertThat(FormatUtil.numberOfDigits(99)).isEqualTo(2);
        assertThat(FormatUtil.numberOfDigits(100)).isEqualTo(3);
        assertThat(FormatUtil.numberOfDigits(1000)).isEqualTo(4);
    }
}
