package de.m3y.hadoop.hdfs.hfsa.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.m3y.hadoop.hdfs.hfsa.util.FsUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.m3y.hadoop.hdfs.hfsa.core.FsImageData.ROOT_PATH;
import static de.m3y.hadoop.hdfs.hfsa.util.FsUtil.isFile;
import static de.m3y.hadoop.hdfs.hfsa.util.FsUtil.isSymlink;

/**
 * Visitor for all files and directories.
 *
 * @see Builder
 */
public interface FsVisitor {
    /**
     * Invoked for each file.
     *
     * @param inode the file inode.
     * @param path  the current path.
     */
    void onFile(FsImageProto.INodeSection.INode inode, String path);

    /**
     * Invoked for each directory.
     *
     * @param inode the directory inode.
     * @param path  the current path.
     */
    void onDirectory(FsImageProto.INodeSection.INode inode, String path);

    /**
     * Invoked for each sym link.
     *
     * @param inode the sym link inode.
     * @param path  the current path.
     */
    void onSymLink(FsImageProto.INodeSection.INode inode, String path);

    /**
     * Builds a visitor with single-threaded (default) or parallel execution.
     * <p>
     * Builder is immutable and creates a new instance if changed.
     */
    class Builder {
        private static final Logger LOG = LoggerFactory.getLogger(de.m3y.hadoop.hdfs.hfsa.core.FsVisitor.Builder.class);
        public static final FsVisitorStrategy DEFAULT_STRATEGY = new FsVisitorDefaultStrategy();
        public static final FsVisitorStrategy PARALLEL_STRATEGY = new FsVisitorParallelStrategy();

        private final FsVisitorStrategy fsVisitorStrategy;

        /**
         * Default constructor.
         */
        public Builder() {
            this(DEFAULT_STRATEGY);
        }

        /**
         * Copy constructor
         *
         * @param fsVisitorStrategy the strategy to visit e.g. in parallel.
         */
        protected Builder(FsVisitorStrategy fsVisitorStrategy) {
            this.fsVisitorStrategy = fsVisitorStrategy;
        }

        public Builder parallel() {
            return new Builder(PARALLEL_STRATEGY);
        }

        public void visit(FsImageData fsImageData, FsVisitor visitor) throws IOException {
            fsVisitorStrategy.visit(fsImageData, visitor);
        }

        public void visit(FsImageData fsImageData, FsVisitor visitor, String path) throws IOException {
            fsVisitorStrategy.visit(fsImageData, visitor, path);
        }

        interface FsVisitorStrategy {
            void visit(FsImageData fsImageData, FsVisitor visitor) throws IOException;

            void visit(FsImageData fsImageData, FsVisitor visitor, String path) throws IOException;
        }

        public static class FsVisitorDefaultStrategy implements FsVisitorStrategy {

            /**
             * Traverses FS tree, starting at root ("{@value FsImageData#ROOT_PATH}").
             *
             * @param fsImageData the FSImage data.
             * @param visitor     the visitor.
             * @throws IOException on error.
             */
            public void visit(FsImageData fsImageData, FsVisitor visitor) throws IOException {
                visit(fsImageData, visitor, ROOT_PATH);
            }

            /**
             * Traverses FS tree, starting at given directory path
             *
             * @param visitor the visitor.
             * @param path    the directory path to start with
             * @throws IOException on error.
             */
            public void visit(FsImageData fsImageData, FsVisitor visitor, String path) throws IOException {
                // Visit path dir
                FsImageProto.INodeSection.INode pathNode = fsImageData.getINodeFromPath(path);
                if (ROOT_PATH.equals(path)) {
                    visitor.onDirectory(pathNode, path);
                } else {
                    // Need to strip current node path from path if not "/"
                    final String substring = path.substring(0, path.length() - pathNode.getName().toStringUtf8().length());
                    visitor.onDirectory(pathNode, substring);
                }

                // Child dirs?
                final long pathNodeId = pathNode.getId();
                final long[] children = fsImageData.getChildINodeIds(pathNodeId);
                // Visit children
                for (long cid : children) {
                    visit(fsImageData, visitor, fsImageData.getInode(cid), path);
                }
            }

            void visit(FsImageData fsImageData, FsVisitor visitor, FsImageProto.INodeSection.INode inode, String path) throws IOException {
                if (FsUtil.isDirectory(inode)) {
                    visitor.onDirectory(inode, path);
                    final long inodeId = inode.getId();
                    final long[] children = fsImageData.getChildINodeIds(inodeId);
                    if (children.length>0) {
                        String newPath;
                        if (ROOT_PATH.equals(path)) {
                            newPath = path + inode.getName().toStringUtf8();
                        } else {
                            newPath = path + '/' + inode.getName().toStringUtf8();
                        }
                        for (long cid : children) {
                            visit(fsImageData, visitor, fsImageData.getInode(cid), newPath);
                        }
                    }
                } else if (isFile(inode)) {
                    visitor.onFile(inode, path);
                } else if (isSymlink(inode)) {
                    visitor.onSymLink(inode, path);
                } else {
                    // Should not happen
                    throw new IllegalStateException("Unsupported inode type " + inode.getType() + " for " + inode);
                }
            }
        }

        public static class FsVisitorParallelStrategy implements FsVisitorStrategy {

            /**
             * Traverses FS tree, using Java parallel stream.
             *
             * @param visitor the visitor.
             * @throws IOException on error.
             */
            public void visit(FsImageData fsImageData, FsVisitor visitor) throws IOException {
                visit(fsImageData, visitor, ROOT_PATH);
            }

            /**
             * Traverses FS tree, using Java parallel stream.
             *
             * @param visitor the visitor.
             * @param path    the directory path to start with
             * @throws IOException on error.
             */
            public void visit(FsImageData fsImageData, FsVisitor visitor, String path) throws IOException {
                FsImageProto.INodeSection.INode rootNode = fsImageData.getINodeFromPath(path);
                visitor.onDirectory(rootNode, path);
                final long rootNodeId = rootNode.getId();
                final long[] children = fsImageData.getChildINodeIds(rootNodeId);
                if (children.length>0) {
                    List<FsImageProto.INodeSection.INode> dirs = new ArrayList<>();
                    for (long cid : children) {
                        final FsImageProto.INodeSection.INode inode = fsImageData.getInode(cid);
                        if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
                            dirs.add(inode);
                        } else {
                            visit(fsImageData, visitor, inode, path);
                        }
                    }
                    // Go over top level dirs in parallel
                    dirs.parallelStream().forEach(inode -> {
                        try {
                            visit(fsImageData, visitor, inode, path);
                        } catch (IOException e) {
                            LOG.error("Can not traverse {} : {}", inode.getId(), inode.getName().toStringUtf8(), e);
                        }
                    });
                }
            }

            void visit(FsImageData fsImageData, FsVisitor visitor, FsImageProto.INodeSection.INode inode, String path) throws IOException {
                if (FsUtil.isDirectory(inode)) {
                    visitor.onDirectory(inode, path);
                    final long inodeId = inode.getId();
                    final long[] children = fsImageData.getChildINodeIds(inodeId);
                    if (children.length > 0) {
                        String newPath;
                        if (ROOT_PATH.equals(path)) {
                            newPath = path + inode.getName().toStringUtf8();
                        } else {
                            newPath = path + '/' + inode.getName().toStringUtf8();
                        }
                        for (long cid : children) {
                            visit(fsImageData, visitor, fsImageData.getInode(cid), newPath);
                        }
                    }
                } else if (isFile(inode)) {
                    visitor.onFile(inode, path);
                } else if (isSymlink(inode)) {
                    visitor.onSymLink(inode, path);
                } else {
                    throw new IllegalStateException("Unsupported inode type " + inode.getType() + " for " + inode);
                }
            }
        }
    }
}
