package de.m3y.hadoop.hdfs.hfsa.util;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
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
     * Gets the file replication honoring erasure coding.
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
     *
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
     * Computes the consumed file size for all blocks.
     *
     * @param file the file.
     * @return the consumed size in bytes.
     */
    public static long getConsumedFileSize(FsImageProto.INodeSection.INodeFile file) {
        long size = 0;
        if (file.hasErasureCodingPolicyID()) {
            ErasureCodingPolicy ecp = SystemErasureCodingPolicies.getByID((byte) file.getErasureCodingPolicyID());
            for (HdfsProtos.BlockProto p : file.getBlocksList()) {
                size += p.getNumBytes();
                double cells = Math.ceil((double) p.getNumBytes() / ecp.getCellSize()); // count of cells
                long rows = (long) Math.ceil(cells / ecp.getNumDataUnits()); // count group of cells (rows)
                size += rows * ecp.getNumParityUnits() * ecp.getCellSize();
            }
        } else {
            size = getFileSize(file) * file.getReplication();
        }
        return size;
    }
}
