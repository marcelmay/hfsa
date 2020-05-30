package de.m3y.hadoop.hdfs.hfsa.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import com.google.protobuf.InvalidProtocolBufferException;
import de.m3y.hadoop.hdfs.hfsa.util.FsUtil;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager;

/**
 * Represents a loaded FSImage.
 */
public class FsImageData {
    public static final String ROOT_PATH = "/";
    public static final char PATH_SEPARATOR = '/';

    private final SerialNumberManager.StringTable stringTable;
    private final FsImageLoader.INodesRepository inodes;
    private final Long2ObjectLinkedOpenHashMap<long[]> dirMap;

    public FsImageData(SerialNumberManager.StringTable stringTable,
                       FsImageLoader.INodesRepository inodes,
                       Long2ObjectLinkedOpenHashMap<long[]> dirMap) {
        this.stringTable = stringTable;
        this.inodes = inodes;
        this.dirMap = dirMap;
    }


    /**
     * Gets the files in given directory.
     *
     * @param path the directory path.
     * @return a list of file inodes, or an empty list.
     * @throws IOException on error, eg FileNotFoundException if path does not exist
     */
    public List<FsImageProto.INodeSection.INode> getFileINodesInDirectory(String path) throws IOException {
        final FsImageProto.INodeSection.INode nodeId = getINodeFromPath(path);
        if (!FsUtil.isDirectory(nodeId)) {
            throw new IllegalArgumentException("Expected directory but <" + path + "> is of type " + nodeId.getType());
        }
        long[] children = dirMap.get(nodeId.getId());
        if (children.length == 0) {
            return Collections.emptyList();
        } else {
            List<FsImageProto.INodeSection.INode> files = new ArrayList<>(children.length);
            for (long cid : children) {
                final FsImageProto.INodeSection.INode inode = inodes.getInode(cid);
                if (FsUtil.isFile(inode)) {
                    files.add(inode);
                }
            }
            return files;
        }
    }

    public FsImageProto.INodeSection.INode getInode(long id) throws InvalidProtocolBufferException {
        return inodes.getInode(id);
    }

    /**
     * Returns the INode of a directory, file or symlink for the specified path.
     *
     * @param path the path of the inode.
     * @return the INode found.
     * @throws IOException on error.
     */
    public FsImageProto.INodeSection.INode getINodeFromPath(String path) throws IOException {
        if (!path.startsWith(ROOT_PATH)) {
            throw new IllegalArgumentException("Expected path <" + path + "> to start with " + PATH_SEPARATOR);
        }
        String normalizedPath = normalizePath(path);
        long id = INodeId.ROOT_INODE_ID;
        // Root node?
        if (ROOT_PATH.equals(normalizedPath)) {
            return inodes.getInode(id);
        }

        // Walk the hierarchy for each path segment
        int startIdx = 1;
        int endIdx = startIdx;
        FsImageProto.INodeSection.INode node = null;
        while (endIdx > 0) {
            endIdx = normalizedPath.indexOf(PATH_SEPARATOR, startIdx);
            String pathSegment = endIdx >= 0 ? normalizedPath.substring(startIdx, endIdx) /* dir */ : normalizedPath.substring(startIdx) /* file */;

            final long[] children = dirMap.get(id);
            if (children.length == 0) {
                throw new FileNotFoundException(path);
            }

            boolean found = false;
            for (long cid : children) {
                node = inodes.getInode(cid);
                if (pathSegment.equals(node.getName().toStringUtf8())) {
                    found = true;
                    id = node.getId();
                    break;
                }
            }
            if (!found) {
                throw new FileNotFoundException(path);
            }

            startIdx = endIdx + 1;
        }

        return node;
    }


    /**
     * Checks if an INode entry (directory, file or symlink) exists for the specified path.
     *
     * @param path the path of the inode.
     * @return true, if exists.
     * @throws IOException on error.
     */
    public boolean hasINode(String path) throws IOException {
        try {
            lookupInodeId(path);
            return true;
        } catch (FileNotFoundException e) {
            // not found
            return false;
        }
    }

    /**
     * Gets the child directory absolute paths for given path.
     *
     * @param path the parent directory path.
     * @return the list of child directory paths.
     * @throws IOException on error, eg FileNotFoundException if path does not exist.
     */
    public List<String> getChildDirectories(String path) throws IOException {
        final long parentNodeId = lookupInodeId(path);
        long[] children = dirMap.get(parentNodeId);
        if (children.length == 0) {
            return Collections.emptyList();
        } else {
            List<String> childPaths = new ArrayList<>();
            final String pathWithTrailingSlash = ROOT_PATH.equals(path) ? path : path + PATH_SEPARATOR;
            for (long cid : children) {
                final FsImageProto.INodeSection.INode inode = inodes.getInode(cid);
                if (FsUtil.isDirectory(inode)) {
                    childPaths.add(pathWithTrailingSlash + inode.getName().toStringUtf8());
                }
            }
            return childPaths;
        }
    }

    /**
     * Checks if directory INode has any children (dirs, files , links).
     * <p>
     * Note: Slower thant {@link #hasChildren(long)}, as path has to be parsed and loaded.
     *
     * @param path the directory path - must exist, or a java.util.NoSuchElementException will be thrown.
     * @return true, if child inodes exist.
     * @throws IOException on error.
     */
    public boolean hasChildren(String path) throws IOException {
        final long rootNodeId = lookupInodeId(path);
        return hasChildren(rootNodeId);
    }

    /**
     * Checks if directory INode has any children (dirs, files , links).
     *
     * @param nodeId the node id.
     * @return true, if child inodes exist.
     */
    public boolean hasChildren(long nodeId) {
        return dirMap.get(nodeId).length > 0;
    }

    /**
     * Return the JSON formatted ACL status of the specified file.
     *
     * @param path a path specifies a file
     * @return JSON formatted AclStatus
     * @throws IOException if failed to serialize fileStatus to JSON.
     */
    public AclStatus getAclStatus(String path) throws IOException {
        PermissionStatus p = getPermissionStatus(path);
        List<AclEntry> aclEntryList = getAclEntryList(path);
        FsPermission permission = p.getPermission();
        AclStatus.Builder builder = new AclStatus.Builder();
        builder.owner(p.getUserName()).group(p.getGroupName())
                .addEntries(aclEntryList).setPermission(permission)
                .stickyBit(permission.getStickyBit());
        return builder.build();
    }

    protected List<AclEntry> getAclEntryList(String path) throws IOException {
        FsImageProto.INodeSection.INode inode = getINodeFromPath(path);
        switch (inode.getType()) {
            case FILE:
                FsImageProto.INodeSection.INodeFile f = inode.getFile();
                return FSImageFormatPBINode.Loader.loadAclEntries(
                        f.getAcl(), stringTable);
            case DIRECTORY:
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                return FSImageFormatPBINode.Loader.loadAclEntries(
                        d.getAcl(), stringTable);
            default:
                return Collections.emptyList();
        }
    }

    /**
     * Gets the permission status for a file or directory or symlink path.
     *
     * @param path the path for a file or directory or symlink.
     * @return the permission status.
     * @throws IOException on error.
     */
    public PermissionStatus getPermissionStatus(String path) throws IOException {
        return getPermissionStatus(getINodeFromPath(path));
    }

    /**
     * Gets the permission status for a file or directory or symlink path.
     *
     * @param inode the path for a file or directory or symlink.
     * @return the permission status.
     */
    public PermissionStatus getPermissionStatus(FsImageProto.INodeSection.INode inode) {
        switch (inode.getType()) {
            case FILE:
                FsImageProto.INodeSection.INodeFile f = inode.getFile();
                return FSImageFormatPBINode.Loader.loadPermission(
                        f.getPermission(), stringTable);
            case DIRECTORY:
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                return FSImageFormatPBINode.Loader.loadPermission(
                        d.getPermission(), stringTable);
            case SYMLINK:
                FsImageProto.INodeSection.INodeSymlink s = inode.getSymlink();
                return FSImageFormatPBINode.Loader.loadPermission(
                        s.getPermission(), stringTable);
            default:
                throw new IllegalStateException("No implementation for getting permission status for type "
                        + inode.getType().name());
        }
    }

    /**
     * Returns the INode Id of the specified path, or if not found throws FileNotFoundException.
     *
     * @param path the path.
     * @return the inode id.
     */
    private long lookupInodeId(String path) throws IOException {
        return getINodeFromPath(path).getId();
    }

    /**
     * Loads the permission status
     *
     * @param permission the permission.
     * @return the  permission status.
     */
    public PermissionStatus getPermissionStatus(long permission) {
        return FSImageFormatPBINode.Loader.loadPermission(permission, stringTable);
    }

    /**
     * Gets the number of INode children.
     *
     * @param inode the inode.
     * @return the number of children or 0 (eg when type FILE or SYMLINK).
     */
    public int getNumChildren(FsImageProto.INodeSection.INode inode) {
        return dirMap.get(inode.getId()).length;
    }

    /**
     * Gets the inode IDs of the children, or empty array.
     *
     * @param pathNodeId the node id of parent directory
     * @return array of child node IDs or empty array.
     */
    public long[] getChildINodeIds(long pathNodeId) {
        return dirMap.get(pathNodeId);
    }

    private static final Pattern DOUBLE_SLASH = Pattern.compile("//+");

    /**
     * Strips [/]+ or trailing slashes.
     *
     * @param path the path to normalize
     * @return the normalized path
     */
    static String normalizePath(String path) {
        String pathWithoutDoubleSlashes = DOUBLE_SLASH.matcher(path).replaceAll("/");
        final int length = pathWithoutDoubleSlashes.length();
        if(length >1 && pathWithoutDoubleSlashes.endsWith("/")) {
            return pathWithoutDoubleSlashes.substring(0, length -1);
        }
        return  pathWithoutDoubleSlashes;
    }
}
