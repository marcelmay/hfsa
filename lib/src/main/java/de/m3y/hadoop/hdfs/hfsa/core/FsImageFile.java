package de.m3y.hadoop.hdfs.hfsa.core;

public class FsImageFile extends FsImageInfo {

  private long accessTime;

  private int replication;

  private int blocksCount;

  private long preferredBlockSize;

  private int storagePolicyId;

  private String blockType;

  private long fileSizeByte;

  public long getFileSizeByte() {
    return fileSizeByte;
  }

  public void setFileSizeByte(long fileSizeByte) {
    this.fileSizeByte = fileSizeByte;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public void setAccessTime(long accessTime) {
    this.accessTime = accessTime;
  }

  public int getReplication() {
    return replication;
  }

  public void setReplication(int replication) {
    this.replication = replication;
  }

  public int getBlocksCount() {
    return blocksCount;
  }

  public void setBlocksCount(int blocksCount) {
    this.blocksCount = blocksCount;
  }

  public long getPreferredBlockSize() {
    return preferredBlockSize;
  }

  public void setPreferredBlockSize(long preferredBlockSize) {
    this.preferredBlockSize = preferredBlockSize;
  }

  public int getStoragePolicyId() {
    return storagePolicyId;
  }

  public void setStoragePolicyId(int storagePolicyId) {
    this.storagePolicyId = storagePolicyId;
  }

  public String getBlockType() {
    return blockType;
  }

  public void setBlockType(String blockType) {
    this.blockType = blockType;
  }
}
