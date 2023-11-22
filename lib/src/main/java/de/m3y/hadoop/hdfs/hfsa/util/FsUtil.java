package de.m3y.hadoop.hdfs.hfsa.util;

import de.m3y.hadoop.hdfs.hfsa.core.FsImageDir;
import de.m3y.hadoop.hdfs.hfsa.core.FsImageFile;
import de.m3y.hadoop.hdfs.hfsa.core.FsImageSymLink;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * Helper for dealing with FSImage and INodes.
 */
public class FsUtil {
    private static final BlockStoragePolicySuite BLOCK_STORAGE_POLICY_SUITE =
            BlockStoragePolicySuite.createDefaultSuite();

    private FsUtil() {
        // No instantiation.
    }

    /**
     * Checks if INode is a directory.
     *
     * @param inode the inode.
     * @return true, if directory.
     */
    public static boolean isDirectory(FsImageProto.INodeSection.INode inode) {
        return inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY;
    }

    /**
     * Checks if INode is a file.
     *
     * @param inode the inode.
     * @return true, if file.
     */
    public static boolean isFile(FsImageProto.INodeSection.INode inode) {
        return inode.getType() == FsImageProto.INodeSection.INode.Type.FILE;
    }

    /**
     * Checks if INode is a symlink.
     *
     * @param inode the inode.
     * @return true, if symlink.
     */
    public static boolean isSymlink(FsImageProto.INodeSection.INode inode) {
        return inode.getType() == FsImageProto.INodeSection.INode.Type.SYMLINK;
    }

    /**
     * Gets the file replication honouring erasure coding.
     *
     * @param file the file
     * @return the replication
     */
    public static int getFileReplication(FsImageProto.INodeSection.INodeFile file) {
        if (file.hasErasureCodingPolicyID()) {
            return INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS;
        }
        return file.getReplication();
    }

    /**
     * Formats the permission as octal.
     * @param permission the permission.
     * @return the formatted octal value.
     */
    public static String toString(FsPermission permission) {
        return String.format("%o", permission.toShort());
    }

    /**
     * Gets the storage policy of a file.
     *
     * @param iNodeFile the file
     * @return the policy (
     */
    public static BlockStoragePolicy getBlockStoragePolicy(FsImageProto.INodeSection.INodeFile iNodeFile) {
        if (iNodeFile.hasStoragePolicyID()) {
            byte policyId = (byte) iNodeFile.getStoragePolicyID();
            return BLOCK_STORAGE_POLICY_SUITE.getPolicy(policyId);
        }
        return BLOCK_STORAGE_POLICY_SUITE.getDefaultPolicy();
    }

    /**
     * Computes the file size for all blocks.
     *
     * @param file the file.
     * @return the size in bytes.
     */
    public static long getFileSize(FsImageProto.INodeSection.INodeFile file) {
        long size = 0;
        for (HdfsProtos.BlockProto p : file.getBlocksList()) {
            size += p.getNumBytes();
        }
        return size;
    }

    /**
     * Extracts the file information in an Inode
     * @param inode the inode
     * @return instance of FsImageFile if inode is a File else null
     */
    public static FsImageFile getFsImageFile(FsImageProto.INodeSection.INode inode) {
        FsImageFile fsImageFile = new FsImageFile();
        fsImageFile.setInode(inode);
        FsImageProto.INodeSection.INodeFile file = inode.getFile();
        fsImageFile.setAccessTime(file.getAccessTime());
        fsImageFile.setBlocksCount(file.getBlocksCount());
        fsImageFile.setReplication(file.getReplication());
        fsImageFile.setBlockType(file.getBlockType().name());
        fsImageFile.setPreferredBlockSize(file.getPreferredBlockSize());
        fsImageFile.setStoragePolicyId(file.getStoragePolicyID());
        fsImageFile.setModificationTime(file.getModificationTime());
        fsImageFile.setInodeId(inode.getId());
        fsImageFile.setName(inode.getName().toStringUtf8());
        fsImageFile.setPermissionId(file.getPermission());
        fsImageFile.setFileSizeByte(FsUtil.getFileSize(file));
        // path, fatherInodeId, user, group, permission, fsImageId to be set
        return fsImageFile;
    }

    /**
     * Extracts the directory information in an Inode
     * @param inode inode of directory
     * @return an instance of FsImageDir
     */
    public static FsImageDir getFsImageDir(FsImageProto.INodeSection.INode inode) {
        org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory iNodeDirectory = inode.getDirectory();
        FsImageDir fsImageDir = new FsImageDir();
        fsImageDir.setInode(inode);
        fsImageDir.setName(inode.getName().toStringUtf8());
        fsImageDir.setDataSpaceQuota(iNodeDirectory.getDsQuota());
        fsImageDir.setNameSpaceQuota(iNodeDirectory.getNsQuota());
        fsImageDir.setPermissionId(iNodeDirectory.getPermission());
        fsImageDir.setModificationTime(iNodeDirectory.getModificationTime());
        // path, fatherInodeId, user, group, permission, fsImageId, totalFileNum, totalFileByte to be set
        return fsImageDir;
    }

    /**
     * Extracts the symbol link information in an Inode
     * @param inode inode of symbol link
     * @return an instance of FsImageSynLink
     */
    public static FsImageSymLink getFsImageSymLink(FsImageProto.INodeSection.INode inode) {
        FsImageProto.INodeSection.INodeSymlink inodeSymlink = inode.getSymlink();
        FsImageSymLink fsImageSymLink = new FsImageSymLink();
        fsImageSymLink.setInode(inode);
        fsImageSymLink.setTarget(inodeSymlink.getTarget());
        fsImageSymLink.setSerializedSize(inodeSymlink.getSerializedSize());
        fsImageSymLink.setAccessTime(inodeSymlink.getAccessTime());
        fsImageSymLink.setInodeId(inode.getId());
        fsImageSymLink.setPermissionId(inodeSymlink.getPermission());
        fsImageSymLink.setModificationTime(inodeSymlink.getModificationTime());
        fsImageSymLink.setName(inode.getName().toStringUtf8());
        // path, fatherInodeId,user, group, permission, fsImageId
        return fsImageSymLink;
    }

}
