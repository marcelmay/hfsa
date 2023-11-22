package de.m3y.hadoop.hdfs.hfsa.core;

public class FsImageDir extends FsImageInfo {
  private long dataSpaceQuota;
  private long nameSpaceQuota;

  private long totalFileNum;  // total file num inside current folder

  private long totalFileByte; // total file size inside current folder

  public long getDataSpaceQuota() {
    return dataSpaceQuota;
  }

  public void setDataSpaceQuota(long dataSpaceQuota) {
    this.dataSpaceQuota = dataSpaceQuota;
  }

  public long getNameSpaceQuota() {
    return nameSpaceQuota;
  }

  public void setNameSpaceQuota(long nameSpaceQuota) {
    this.nameSpaceQuota = nameSpaceQuota;
  }

  public long getTotalFileNum() {
    return totalFileNum;
  }

  public void setTotalFileNum(long totalFileNum) {
    this.totalFileNum = totalFileNum;
  }

  public long getTotalFileByte() {
    return totalFileByte;
  }

  public void setTotalFileByte(long totalFileByte) {
    this.totalFileByte = totalFileByte;
  }
}
