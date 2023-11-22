package de.m3y.hadoop.hdfs.hfsa.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import de.m3y.hadoop.hdfs.hfsa.util.FsUtil;
import org.apache.hadoop.fs.permission.PermissionStatus;
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
     * @param fsImageFile the file inode entity.
     */
    void onFile(FsImageFile fsImageFile);

    /**
     * Invoked for each directory.
     *
     * @param fsImageDir the directory inode entity.
     */
    void onDirectory(FsImageDir fsImageDir);

    /**
     * Invoked for each sym link.
     *
     * @param fsImageSymLink the symlink inode entity.
     */
    void onSymLink(FsImageSymLink fsImageSymLink);

    /**
     * Contains statistic information of a folder inode.
     */
    class Summary {
        public final LongAdder totalFileNum;
        public final LongAdder totalFileByte;

        public Summary(LongAdder totalFileNum, LongAdder totalFileByte) {
            this.totalFileNum = totalFileNum;
            this.totalFileByte = totalFileByte;
        }
        public Summary () {
            totalFileByte = new LongAdder();
            totalFileNum = new LongAdder();
        }
    }

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
             * @param fsImageData the fsImageDataObject parsed
             * @param visitor the visitor.
             * @param path    the directory path to start with
             * @throws IOException on error.
             */
            public void visit(FsImageData fsImageData, FsVisitor visitor, String path) throws IOException {
                // Visit path dir
                FsImageProto.INodeSection.INode pathNode = fsImageData.getINodeFromPath(path);
                Summary summary = new Summary(new LongAdder(), new LongAdder());
                FsImageDir fsImageDir = FsUtil.getFsImageDir(pathNode);
                fsImageDir.setPath(path);
                fsImageDir.setFatherInodeId(null);
                final long permissionId = fsImageDir.getPermissionId();
                PermissionStatus permissionStatus = fsImageData.getPermissionStatus(permissionId);
                fsImageDir.setPermission(permissionStatus.getPermission().toString());
                fsImageDir.setUser(permissionStatus.getUserName());
                fsImageDir.setGroup(permissionStatus.getGroupName());
                // Child dirs?
                final long pathNodeId = pathNode.getId();
                final long[] children = fsImageData.getChildINodeIds(pathNodeId);
                // Visit children
                for (long cid : children) {
                    Summary subSummery = visit(fsImageData, visitor, fsImageData.getInode(cid), fsImageDir.getPath(), pathNode.getId());
                    summary.totalFileByte.add(subSummery.totalFileByte.longValue());
                    summary.totalFileNum.add(subSummery.totalFileNum.longValue());
                }
                fsImageDir.setTotalFileNum(summary.totalFileNum.longValue());
                fsImageDir.setTotalFileByte(summary.totalFileByte.longValue());
                visitor.onDirectory(fsImageDir);
            }

            Summary visit(FsImageData fsImageData, FsVisitor visitor, FsImageProto.INodeSection.INode inode, String path, Long fatherInodeId) throws IOException {
                Summary summary = new Summary(new LongAdder(), new LongAdder());
                if (FsUtil.isDirectory(inode)) {
                    FsImageDir fsImageDir = FsUtil.getFsImageDir(inode);
                    // path is the current path
                    if (ROOT_PATH.equals(path)) {
                        fsImageDir.setPath(ROOT_PATH + fsImageDir.getName());
                    } else {
                        fsImageDir.setPath(path + ROOT_PATH + fsImageDir.getName());
                    }
                    final long permissionId = fsImageDir.getPermissionId();
                    PermissionStatus permissionStatus = fsImageData.getPermissionStatus(permissionId);
                    fsImageDir.setPermission(permissionStatus.getPermission().toString());
                    fsImageDir.setUser(permissionStatus.getUserName());
                    fsImageDir.setGroup(permissionStatus.getGroupName());
                    fsImageDir.setFatherInodeId(fatherInodeId);
                    final long inodeId = inode.getId();
                    final long[] children = fsImageData.getChildINodeIds(inodeId);
                    if (children.length > 0) {
                        String newPath;
                        if (ROOT_PATH.equals(path)) {
                            newPath = path + inode.getName().toStringUtf8();
                        } else {
                            newPath = path + FsImageData.PATH_SEPARATOR + inode.getName().toStringUtf8();
                        }
                        for (long cid : children) {
                            Summary subSummary = visit(fsImageData, visitor, fsImageData.getInode(cid), newPath, inodeId);
                            summary.totalFileByte.add(subSummary.totalFileByte.longValue());
                            summary.totalFileNum.add(subSummary.totalFileNum.longValue());
                        }
                    }
                    fsImageDir.setTotalFileByte(summary.totalFileByte.longValue());
                    fsImageDir.setTotalFileNum(summary.totalFileNum.longValue());
                    visitor.onDirectory(fsImageDir);
                    return summary;
                } else if (isFile(inode)) {
                    FsImageFile fsImageFile = FsUtil.getFsImageFile(inode);
                    final long permissionId = fsImageFile.getPermissionId();
                    PermissionStatus permissionStatus = fsImageData.getPermissionStatus(permissionId);
                    String newPath;
                    if (ROOT_PATH.equals(path)) {
                        newPath = path + inode.getName().toStringUtf8();
                    } else {
                        newPath = path + FsImageData.PATH_SEPARATOR + inode.getName().toStringUtf8();
                    }
                    fsImageFile.setPath(newPath);
                    fsImageFile.setFatherInodeId(fatherInodeId);
                    fsImageFile.setPermission(permissionStatus.getPermission().toString());
                    fsImageFile.setUser(permissionStatus.getUserName());
                    fsImageFile.setGroup(permissionStatus.getGroupName());
                    summary.totalFileNum.increment();
                    summary.totalFileByte.add(fsImageFile.getFileSizeByte());
                    visitor.onFile(fsImageFile);
                    return summary;
                } else if (isSymlink(inode)) {
                    FsImageSymLink fsImageSymLink = FsUtil.getFsImageSymLink(inode);
                    final long permissionId = fsImageSymLink.getPermissionId();
                    PermissionStatus permissionStatus = fsImageData.getPermissionStatus(permissionId);
                    fsImageSymLink.setPermission(permissionStatus.getPermission().toString());
                    fsImageSymLink.setUser(permissionStatus.getUserName());
                    fsImageSymLink.setGroup(permissionStatus.getGroupName());
                    fsImageSymLink.setFatherInodeId(fatherInodeId);
                    fsImageSymLink.setPath(String.format("%s"+FsImageData.PATH_SEPARATOR+"%s", path, fsImageSymLink.getName()));
                    visitor.onSymLink(fsImageSymLink);
                    // it is a link without occupation on file num or file size
                    return summary;
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
                FsImageDir fsImageDir = FsUtil.getFsImageDir(rootNode);
                // path is the current path
                fsImageDir.setPath(path);
                final long permissionId = fsImageDir.getPermissionId();
                PermissionStatus permissionStatus = fsImageData.getPermissionStatus(permissionId);
                fsImageDir.setPermission(permissionStatus.getPermission().toString());
                fsImageDir.setUser(permissionStatus.getUserName());
                fsImageDir.setGroup(permissionStatus.getGroupName());
                fsImageDir.setFatherInodeId(null);
                Summary summary = new Summary(new LongAdder(), new LongAdder());
                final long rootNodeId = rootNode.getId();
                final long[] children = fsImageData.getChildINodeIds(rootNodeId);
                if (children.length>0) {
                    List<FsImageProto.INodeSection.INode> dirs = new ArrayList<>();
                    for (long cid : children) {
                        final FsImageProto.INodeSection.INode inode = fsImageData.getInode(cid);
                        if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
                            dirs.add(inode);
                        } else {
                            Summary subSummary = visit(fsImageData, visitor, inode, path, rootNodeId);
                            summary.totalFileByte.add(subSummary.totalFileByte.longValue());
                            summary.totalFileNum.add(subSummary.totalFileNum.longValue());
                        }
                    }
                    // Go over top level dirs in parallel
                    dirs.parallelStream().forEach(inode -> {
                        try {
                            Summary subSummary = visit(fsImageData, visitor, inode, path, rootNodeId);
                            summary.totalFileNum.add(subSummary.totalFileNum.longValue());
                            summary.totalFileByte.add(subSummary.totalFileByte.longValue());

                        } catch (IOException e) {
                            LOG.error("Can not traverse {} : {}", inode.getId(), inode.getName().toStringUtf8(), e);
                        }
                    });
                }
                fsImageDir.setTotalFileNum(summary.totalFileNum.longValue());
                fsImageDir.setTotalFileByte(summary.totalFileByte.longValue());
                visitor.onDirectory(fsImageDir);
            }

            Summary visit(FsImageData fsImageData, FsVisitor visitor, FsImageProto.INodeSection.INode inode, String path, Long fatherInodeId) throws IOException {
                Summary summary = new Summary(new LongAdder(), new LongAdder());
                if (FsUtil.isDirectory(inode)) {
                    final long inodeId = inode.getId();
                    final long[] children = fsImageData.getChildINodeIds(inodeId);
                    FsImageDir fsImageDir = FsUtil.getFsImageDir(inode);
                    // path is the current path
                    if (ROOT_PATH.equals(path)) {
                        fsImageDir.setPath(ROOT_PATH + fsImageDir.getName());
                    } else {
                        fsImageDir.setPath(path + ROOT_PATH + fsImageDir.getName());
                    }
                    long permissionId = fsImageDir.getPermissionId();
                    PermissionStatus permissionStatus = fsImageData.getPermissionStatus(permissionId);
                    fsImageDir.setPermission(permissionStatus.getPermission().toString());
                    fsImageDir.setUser(permissionStatus.getUserName());
                    fsImageDir.setGroup(permissionStatus.getGroupName());
                    fsImageDir.setFatherInodeId(fatherInodeId);
                    if (children.length>0) {
                        String newPath;
                        if (ROOT_PATH.equals(path)) {
                            newPath = path + inode.getName().toStringUtf8();
                        } else {
                            newPath = path + FsImageData.PATH_SEPARATOR + inode.getName().toStringUtf8();
                        }
                        for (long cid : children) {
                            Summary subSummary = visit(fsImageData, visitor, fsImageData.getInode(cid), newPath, inodeId);
                            summary.totalFileByte.add(subSummary.totalFileByte.longValue());
                            summary.totalFileNum.add(subSummary.totalFileNum.longValue());
                        }
                    }
                    fsImageDir.setTotalFileByte(summary.totalFileByte.longValue());
                    fsImageDir.setTotalFileNum(summary.totalFileNum.longValue());
                    visitor.onDirectory(fsImageDir);
                    return summary;
                } else if (isFile(inode)) {
                    FsImageFile fsImageFile = FsUtil.getFsImageFile(inode);
                    long permissionId = fsImageFile.getPermissionId();
                    PermissionStatus permissionStatus = fsImageData.getPermissionStatus(permissionId);
                    String newPath;
                    if (ROOT_PATH.equals(path)) {
                        newPath = path + inode.getName().toStringUtf8();
                    } else {
                        newPath = path + FsImageData.PATH_SEPARATOR + inode.getName().toStringUtf8();
                    }
                    fsImageFile.setPath(newPath);
                    fsImageFile.setFatherInodeId(fatherInodeId);
                    fsImageFile.setPermission(permissionStatus.getPermission().toString());
                    fsImageFile.setUser(permissionStatus.getUserName());
                    fsImageFile.setGroup(permissionStatus.getGroupName());
                    summary.totalFileNum.increment();
                    summary.totalFileByte.add(fsImageFile.getFileSizeByte());
                    visitor.onFile(fsImageFile);
                    return summary;
                } else if (isSymlink(inode)) {
                    FsImageSymLink fsImageSymLink = FsUtil.getFsImageSymLink(inode);
                    long permissionId = fsImageSymLink.getPermissionId();
                    PermissionStatus permissionStatus = fsImageData.getPermissionStatus(permissionId);
                    fsImageSymLink.setPermission(permissionStatus.getPermission().toString());
                    fsImageSymLink.setUser(permissionStatus.getUserName());
                    fsImageSymLink.setGroup(permissionStatus.getGroupName());
                    fsImageSymLink.setFatherInodeId(fatherInodeId);
                    fsImageSymLink.setPath(String.format("%s"+FsImageData.PATH_SEPARATOR+"%s", path, fsImageSymLink.getName()));
                    visitor.onSymLink(fsImageSymLink);
                    // it is a link without occupation on file num or file size
                    return summary;
                } else {
                    throw new IllegalStateException("Unsupported inode type " + inode.getType() + " for " + inode);
                }
            }
        }
    }
}
