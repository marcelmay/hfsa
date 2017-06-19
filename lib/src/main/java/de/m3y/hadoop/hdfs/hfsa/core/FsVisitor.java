package de.m3y.hadoop.hdfs.hfsa.core;

import org.apache.hadoop.hdfs.server.namenode.FsImageProto;

/**
 * Visitor for all files and directories.
 *
 * @see FSImageLoader#visit(FsVisitor)
 */
public interface FsVisitor {
    /**
     * Invoked for each file.
     *
     * @param inode the file inode.
     */
    void onFile(FsImageProto.INodeSection.INode inode);

    /**
     * Invoked for each directory.
     *
     * @param inode the directory inode.
     */
    void onDirectory(FsImageProto.INodeSection.INode inode);

    /**
     * Invoked for each sym link.
     * @param inode the sym link inode.
     */
    void onSymLink(FsImageProto.INodeSection.INode inode);
}
