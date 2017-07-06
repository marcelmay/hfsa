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
     * @param path the current path.
     */
    void onFile(FsImageProto.INodeSection.INode inode, String path);

    /**
     * Invoked for each directory.
     *
     * @param inode the directory inode.
     * @param path the current path.
     */
    void onDirectory(FsImageProto.INodeSection.INode inode, String path);

    /**
     * Invoked for each sym link.
     *
     * @param inode the sym link inode.
     * @param path the current path.
     */
    void onSymLink(FsImageProto.INodeSection.INode inode, String path);
}
