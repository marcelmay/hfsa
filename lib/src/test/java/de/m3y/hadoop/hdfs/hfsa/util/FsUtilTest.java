package de.m3y.hadoop.hdfs.hfsa.util;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FsUtilTest {
    @Test
    public void testToStringFsPermission() {
        FsPermission fsPermission = FsPermission.valueOf("-rw-r--r--");
        assertThat(FsUtil.toString(fsPermission)).isEqualTo("0644");
    }
    @Test
    public void testToStringPermissionStatus() {
        PermissionStatus permissionStatus = PermissionStatus.createImmutable("foo", "bar",
                FsPermission.valueOf("-rw-r--r--"));
        assertThat(FsUtil.toString(permissionStatus)).isEqualTo("foo:bar:0644");
    }
}