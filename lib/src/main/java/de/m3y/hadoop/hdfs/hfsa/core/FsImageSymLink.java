package de.m3y.hadoop.hdfs.hfsa.core;

import org.apache.hadoop.thirdparty.protobuf.ByteString;
public class FsImageSymLink extends FsImageInfo {
  private int serializedSize;

  private ByteString target;

  private long accessTime;

  public int getSerializedSize() {
    return serializedSize;
  }

  public void setSerializedSize(int serializedSize) {
    this.serializedSize = serializedSize;
  }

  public ByteString getTarget() {
    return target;
  }

  public void setTarget(ByteString target) {
    this.target = target;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public void setAccessTime(long accessTime) {
    this.accessTime = accessTime;
  }
}
