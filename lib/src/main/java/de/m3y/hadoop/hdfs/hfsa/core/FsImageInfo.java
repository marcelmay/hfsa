package de.m3y.hadoop.hdfs.hfsa.core;

import org.apache.hadoop.hdfs.server.namenode.FsImageProto;

public class FsImageInfo {
  private Long fsImageId;
  private Long inodeId;
  private Long fatherInodeId;  // father folder inode id
  private String path;
  private String name;
  private String user;
  private String group;
  private String permission;
  private long modificationTime;
  private FsImageProto.INodeSection.INode inode;
  private long permissionId;

  public long getPermissionId() {
    return permissionId;
  }

  public void setPermissionId(long permissionId) {
    this.permissionId = permissionId;
  }

  public FsImageProto.INodeSection.INode getInode() {
    return inode;
  }

  public void setInode(FsImageProto.INodeSection.INode inode) {
    this.inode = inode;
  }

  public Long getFsImageId() {
    return fsImageId;
  }

  public void setFsImageId(Long fsImageId) {
    this.fsImageId = fsImageId;
  }

  public Long getInodeId() {
    return inodeId;
  }

  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }

  public Long getFatherInodeId() {
    return fatherInodeId;
  }

  public void setFatherInodeId(Long fatherInodeId) {
    this.fatherInodeId = fatherInodeId;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public String getPermission() {
    return permission;
  }

  public void setPermission(String permission) {
    this.permission = permission;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }
}
