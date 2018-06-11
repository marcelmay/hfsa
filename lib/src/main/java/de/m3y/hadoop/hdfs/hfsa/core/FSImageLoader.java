/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.m3y.hadoop.hdfs.hfsa.core;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.CodedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.LimitInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FSImageLoader loads fsimage and provide methods to return
 * file status of the namespace of the fsimage.
 * <p>
 * Note: This class is based on the original FSImageLoader from Hadoop:
 * https://github.com/apache/hadoop/blob/master/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/FSImageLoader.java
 */
public class FSImageLoader {
    private static final Logger LOG = LoggerFactory.getLogger(FSImageLoader.class);
    public static final String ROOT_PATH = "/";

    private final String[] stringTable;
    // byte representation of inodes, sorted by id
    private final byte[][] inodes;
    // inodesIdxToIdCache contains the INode ID, to avoid redundant parsing when using fromINodeId
    private final long[] inodesIdxToIdCache;
    private final Map<Long, long[]> dirmap;
    private static final Comparator<byte[]> INODE_BYTES_COMPARATOR = (o1, o2) -> {
        try {
            return Long.compare(extractNodeId(o1), extractNodeId(o2));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    };

    private static long extractNodeId(byte[] buf) throws IOException {
        // Pretty much of a hack, as Protobuf 2.5 does not partial parsing
        // In a micro benchmark, it is several times(!) faster
        // FsImageProto.INodeSection.INode.parseFrom(o2).getId()
        CodedInputStream input = CodedInputStream.newInstance(buf, 0, buf.length);
        int tag = input.readTag();
        if (tag != 8) {
            throw new IllegalStateException("Can not parse type enum from INode, got tag " + tag + " but expected " + 8);
        }
        input.readEnum(); // Ignore
        tag = input.readTag();
        if (tag != 16) {
            throw new IllegalStateException("Can not parse type enum from INode, got tag " + tag + " but expected " + 16);
        }
        return input.readUInt64();
    }

    private static final SectionComparator SECTION_COMPARATOR = new SectionComparator();

    private static class SectionComparator implements Comparator<FsImageProto.FileSummary.Section> {
        @Override
        public int compare(FsImageProto.FileSummary.Section s1,
                           FsImageProto.FileSummary.Section s2) {
            FSImageFormatProtobuf.SectionName n1 =
                    FSImageFormatProtobuf.SectionName.fromString(s1.getName());
            FSImageFormatProtobuf.SectionName n2 =
                    FSImageFormatProtobuf.SectionName.fromString(s2.getName());
            if (n1 == null) {
                return n2 == null ? 0 : -1;
            } else if (n2 == null) {
                return -1;
            } else {
                return n1.ordinal() - n2.ordinal();
            }
        }
    }

    private FSImageLoader(String[] stringTable, byte[][] inodes,
                          Map<Long, long[]> dirmap) {
        this.stringTable = stringTable;
        this.inodes = inodes;
        this.inodesIdxToIdCache = new long[inodes.length];
        for (int i = 0; i < inodesIdxToIdCache.length; i++) {
            try {
                inodesIdxToIdCache[i] = extractNodeId(inodes[i]);
            } catch (IOException e) {
                throw new IllegalStateException("Can not parse inode " + i);
            }
        }
        this.dirmap = dirmap;
    }

    /**
     * Load fsimage into the memory.
     *
     * @param file the filepath of the fsimage to load.
     * @return FSImageLoader
     * @throws IOException if failed to load fsimage.
     */
    public static FSImageLoader load(RandomAccessFile file) throws IOException {
        Configuration conf = new Configuration();

        if (!FSImageUtil.checkFileFormat(file)) {
            throw new IOException("Unrecognized FSImage " + file);
        }

        FsImageProto.FileSummary summary = FSImageUtil.loadSummary(file);
        try (FileInputStream fin = new FileInputStream(file.getFD())) {
            // Map to record INodeReference to the referred id
            ImmutableList<Long> refIdList = null;
            String[] stringTable = null;
            byte[][] inodes = null;
            Map<Long, long[]> dirmap = null;

            List<FsImageProto.FileSummary.Section> sections = summary.getSectionsList().stream()
                    .filter(s -> {
                        final String sectionName = s.getName();
                        return FSImageFormatProtobuf.SectionName.STRING_TABLE.name().equals(sectionName) ||
                                FSImageFormatProtobuf.SectionName.INODE.name().equals(sectionName) ||
                                FSImageFormatProtobuf.SectionName.INODE_REFERENCE.name().equals(sectionName) ||
                                FSImageFormatProtobuf.SectionName.INODE_DIR.name().equals(sectionName);
                    })
                    .sorted(SECTION_COMPARATOR)
                    .collect(Collectors.toList());
            for (FsImageProto.FileSummary.Section s : sections) {
                fin.getChannel().position(s.getOffset());
                if (s.getLength() == 0) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping empty section {} of length {}", s.getName(), s.getLength());
                    }
                } else {
                    InputStream is = FSImageUtil.wrapInputStreamForCompression(conf, summary.getCodec(),
                            new BufferedInputStream(new LimitInputStream(fin, s.getLength()), 8 * 8192 /* 64KiB */));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Loading section {} of length {}", s.getName(), s.getLength());
                    }
                    switch (FSImageFormatProtobuf.SectionName.fromString(s.getName())) {
                        case STRING_TABLE:
                            stringTable = loadStringTable(is);
                            break;
                        case INODE:
                            inodes = loadINodeSection(is);
                            break;
                        case INODE_REFERENCE:
                            refIdList = loadINodeReferenceSection(is);
                            break;
                        case INODE_DIR:
                            dirmap = loadINodeDirectorySection(is, refIdList);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected section " + s.getName());
                    }
                }
            }
            return new FSImageLoader(stringTable, inodes, dirmap);
        }
    }

    private static Map<Long, long[]> loadINodeDirectorySection
            (InputStream in, List<Long> refIdList)
            throws IOException {
        long start = System.currentTimeMillis();
        Map<Long, long[]> dirs = Maps.newHashMapWithExpectedSize(512 * 1014 /* 512K */);
        while (true) {
            FsImageProto.INodeDirectorySection.DirEntry e =
                    FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
            // note that in is a LimitedInputStream
            if (e == null) {
                break;
            }

            final int childrenCount = e.getChildrenCount();
            final long[] l = new long[childrenCount + e.getRefChildrenCount()];
            for (int i = 0; i < childrenCount; ++i) {
                l[i] = e.getChildren(i);
            }
            for (int i = childrenCount; i < l.length; i++) {
                int refId = e.getRefChildren(i - childrenCount);
                l[i] = refIdList.get(refId);
            }
            dirs.put(e.getParent(), l);
        }
        LOG.info("Loaded {} directories [{}ms]", dirs.size(), System.currentTimeMillis() - start);
        return dirs;
    }

    private static ImmutableList<Long> loadINodeReferenceSection(InputStream in)
            throws IOException {
        long startTime = System.currentTimeMillis();
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        long counter = 0;
        while (true) {
            FsImageProto.INodeReferenceSection.INodeReference e =
                    FsImageProto.INodeReferenceSection.INodeReference
                            .parseDelimitedFrom(in);
            if (e == null) {
                break;
            }
            ++counter;
            builder.add(e.getReferredId());
        }
        LOG.info("Loaded {} inode references [{}ms]", counter, System.currentTimeMillis() - startTime);
        return builder.build();
    }

    private static byte[][] loadINodeSection(InputStream in)
            throws IOException {
        long start = System.currentTimeMillis();
        FsImageProto.INodeSection s = FsImageProto.INodeSection
                .parseDelimitedFrom(in);
        final byte[][] inodes = new byte[(int) s.getNumInodes()][];
        for (int i = 0; i < s.getNumInodes(); ++i) {
            int size = CodedInputStream.readRawVarint32(in.read(), in);
            byte[] bytes = new byte[size];
            IOUtils.readFully(in, bytes, 0, size);
            inodes[i] = bytes;
        }
        LOG.info("Loaded {} inodes [{}ms]", s.getNumInodes(), System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        Arrays.parallelSort(inodes, INODE_BYTES_COMPARATOR);
        LOG.info("Sorted {} inodes [{}ms]", inodes.length, System.currentTimeMillis() - start);
        return inodes;
    }

    static String[] loadStringTable(InputStream in) throws
            IOException {
        long start = System.currentTimeMillis();
        FsImageProto.StringTableSection s = FsImageProto.StringTableSection
                .parseDelimitedFrom(in);
        String[] stringTable = new String[s.getNumEntry() + 1];
        for (int i = 0; i < s.getNumEntry(); ++i) {
            FsImageProto.StringTableSection.Entry e = FsImageProto
                    .StringTableSection.Entry.parseDelimitedFrom(in);
            stringTable[e.getId()] = e.getStr();
        }
        LOG.info("Loaded {} strings [{}ms]", s.getNumEntry(), System.currentTimeMillis() - start);
        return stringTable;
    }

    /**
     * Traverses FS tree, starting at root ("/").
     *
     * @param visitor the visitor.
     * @throws IOException on error.
     */
    public void visit(FsVisitor visitor) throws IOException {
        visit(visitor, ROOT_PATH);
    }

    /**
     * Traverses FS tree, starting at given directory path
     *
     * @param visitor the visitor.
     * @param path    the directory path to start with
     * @throws IOException on error.
     */
    public void visit(FsVisitor visitor, String path) throws IOException {
        // Visit path dir
        FsImageProto.INodeSection.INode pathNode = getINodeFromPath(path);
        if (ROOT_PATH.equals(path)) {
            visitor.onDirectory(pathNode, path);
        } else {
            // Need to strip current node path from path if not "/"
            final String substring = path.substring(0, path.length() - pathNode.getName().toStringUtf8().length());
            visitor.onDirectory(pathNode, substring);
        }

        // Child dirs?
        final long pathNodeId = pathNode.getId();
        long[] children = dirmap.get(pathNodeId);
        if (null != children) {
            // Visit children
            for (long cid : children) {
                visit(visitor, cid, path);
            }
        }
    }


    /**
     * Traverses FS tree, using Java parallel stream.
     *
     * @param visitor the visitor.
     * @throws IOException on error.
     */
    public void visitParallel(FsVisitor visitor) throws IOException {
        visitParallel(visitor, ROOT_PATH);
    }

    /**
     * Traverses FS tree, using Java parallel stream.
     *
     * @param visitor the visitor.
     * @param path    the directory path to start with
     * @throws IOException on error.
     */
    public void visitParallel(FsVisitor visitor, String path) throws IOException {
        FsImageProto.INodeSection.INode rootNode = getINodeFromPath(path);
        visitor.onDirectory(rootNode, path);
        final long rootNodeId = rootNode.getId();
        final long[] children = dirmap.get(rootNodeId);
        if (null != children) {
            List<FsImageProto.INodeSection.INode> dirs = new ArrayList<>();
            for (long cid : children) {
                final FsImageProto.INodeSection.INode inode = fromINodeId(cid);
                if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
                    dirs.add(inode);
                } else {
                    visit(visitor, inode, (ROOT_PATH.equals(path) ? path : path + '/') + inode.getName().toStringUtf8());
                }
            }
            dirs.parallelStream().forEach(inode -> {
                try {
                    visit(visitor, inode, path);
                } catch (IOException e) {
                    LOG.error("Can not traverse " + inode.getId() + " : " + inode.getName().toStringUtf8(), e);
                }
            });
        }
    }

    void visit(FsVisitor visitor, long nodeId, String path) throws IOException {
        final FsImageProto.INodeSection.INode inode = fromINodeId(nodeId);
        visit(visitor, inode, path);
    }

    void visit(FsVisitor visitor, FsImageProto.INodeSection.INode inode, String path) throws IOException {
        if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
            visitor.onDirectory(inode, path);
            final long inodeId = inode.getId();
            final long[] children = dirmap.get(inodeId);
            if (null != children) {
                String newPath;
                if (ROOT_PATH.equals(path)) {
                    newPath = path + inode.getName().toStringUtf8();
                } else {
                    newPath = path + '/' + inode.getName().toStringUtf8();
                }
                for (long cid : children) {
                    visit(visitor, cid, newPath);
                }
            }
        } else if (inode.getType() == FsImageProto.INodeSection.INode.Type.FILE) {
            visitor.onFile(inode, path);
        } else if (inode.getType() == FsImageProto.INodeSection.INode.Type.SYMLINK) {
            visitor.onSymLink(inode, path);
        } else {
            // Should not happen
            throw new IllegalStateException("Unsupported inode type " + inode.getType() + " for " + inode);
        }
    }

    /**
     * Gets the files in given directory.
     *
     * @param path the directory path.
     * @return a list of file inodes, or an empty list.
     * @throws IOException on error, eg FileNotFoundException if path does not exist
     */
    public List<FsImageProto.INodeSection.INode> getFileINodesInDirectory(String path) throws IOException {
        final long nodeId = lookup(path);
        long[] children = dirmap.get(nodeId);
        if (null == children) {
            throw new IllegalArgumentException("Path " + path + " is invalid");
        }

        List<FsImageProto.INodeSection.INode> files = new ArrayList<>(children.length);
        for (long cid : children) {
            final FsImageProto.INodeSection.INode inode = fromINodeId(cid);
            if (inode.getType() == FsImageProto.INodeSection.INode.Type.FILE) {
                files.add(inode);
            }
        }
        return files;
    }

    /**
     * Returns the INode of a directory, file or symlink for the specified path.
     *
     * @param path the path of the inode.
     * @return the INode found.
     * @throws IOException on error.
     */
    public FsImageProto.INodeSection.INode getINodeFromPath(String path) throws IOException {
        Preconditions.checkArgument(path.startsWith(ROOT_PATH));
        String normalizedPath = normalizePath(path);
        long id = INodeId.ROOT_INODE_ID;
        // Root node?
        if (ROOT_PATH.equals(normalizedPath)) {
            return fromINodeId(id);
        }

        // Search path
        FsImageProto.INodeSection.INode node = null;
        for (int offset = 0, next; offset < normalizedPath.length(); offset = next) {
            next = normalizedPath.indexOf('/', offset + 1);
            if (next == -1) {
                next = normalizedPath.length();
            }
            if (offset + 1 > next) {
                break;
            }

            final String component = normalizedPath.substring(offset + 1, next);

            if (component.isEmpty()) {
                continue;
            }

            final long[] children = dirmap.get(id);
            if (children == null) {
                throw new FileNotFoundException(path);
            }

            boolean found = false;
            for (long cid : children) {
                node = fromINodeId(cid);
                if (component.equals(node.getName().toStringUtf8())) {
                    found = true;
                    id = node.getId();
                    break;
                }
            }
            if (!found) {
                throw new FileNotFoundException(path);
            }
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
            lookup(path);
            return true;
        } catch (FileNotFoundException e) {
            // not found
        }
        return false;
    }

    /**
     * Gets the child directory absolute paths for given path.
     *
     * @param path the parent directory path.
     * @return the list of child directory paths.
     * @throws IOException on error.
     */
    public List<String> getChildPaths(String path) throws IOException {
        final long rootNodeId = lookup(path);
        long[] children = dirmap.get(rootNodeId);
        if (null == children) {
            throw new NoSuchElementException("No node found for path " + path);
        }
        List<String> childPaths = new ArrayList<>();
        final String pathWithTrailingSlash = ROOT_PATH.equals(path) ? path : path + '/';
        for (long cid : children) {
            final FsImageProto.INodeSection.INode inode = fromINodeId(cid);
            if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
                childPaths.add(pathWithTrailingSlash + inode.getName().toStringUtf8());
            }
        }
        return childPaths;
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
        final long rootNodeId = lookup(path);
        return hasChildren(rootNodeId);
    }

    /**
     * Checks if directory INode has any children (dirs, files , links).
     *
     * @param nodeId the node id.
     * @return true, if child inodes exist.
     */
    public boolean hasChildren(long nodeId) {
        long[] children = dirmap.get(nodeId);
        return null != children && children.length > 0;
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

    private List<AclEntry> getAclEntryList(String path) throws IOException {
        FsImageProto.INodeSection.INode inode = getINodeFromPath(path);
        switch (inode.getType()) {
            case FILE: {
                FsImageProto.INodeSection.INodeFile f = inode.getFile();
                return FSImageFormatPBINode.Loader.loadAclEntries(
                        f.getAcl(), stringTable);
            }
            case DIRECTORY: {
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                return FSImageFormatPBINode.Loader.loadAclEntries(
                        d.getAcl(), stringTable);
            }
            default: {
                return new ArrayList<>();
            }
        }
    }

    public PermissionStatus getPermissionStatus(String path) throws IOException {
        FsImageProto.INodeSection.INode inode = getINodeFromPath(path);
        switch (inode.getType()) {
            case FILE: {
                FsImageProto.INodeSection.INodeFile f = inode.getFile();
                return FSImageFormatPBINode.Loader.loadPermission(
                        f.getPermission(), stringTable);
            }
            case DIRECTORY: {
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                return FSImageFormatPBINode.Loader.loadPermission(
                        d.getPermission(), stringTable);
            }
            case SYMLINK: {
                FsImageProto.INodeSection.INodeSymlink s = inode.getSymlink();
                return FSImageFormatPBINode.Loader.loadPermission(
                        s.getPermission(), stringTable);
            }
            default: {
                throw new IllegalStateException("No implementation for getting permission status for type " + inode.getType().name());
            }
        }
    }

    /**
     * Returns the INodeId of the specified path, or if not found throws FileNotFoundException.
     *
     * @param path the path.
     * @return the inode id.
     */
    private long lookup(String path) throws IOException {
        return getINodeFromPath(path).getId();
    }

    public PermissionStatus getPermissionStatus(long permission) {
        return FSImageFormatPBINode.Loader.loadPermission(permission, stringTable);
    }

    public static long getFileSize(FsImageProto.INodeSection.INodeFile f) {
        long size = 0;
        for (HdfsProtos.BlockProto p : f.getBlocksList()) {
            size += p.getNumBytes();
        }
        return size;
    }

    public static String toString(FsPermission permission) {
        return String.format("%o", permission.toShort());
    }

    private FsImageProto.INodeSection.INode fromINodeId(final long id) throws IOException {
        int l = 0;
        int r = inodes.length;
        while (l < r) {
            int mid = l + (r - l) / 2;
            long nid = inodesIdxToIdCache[mid];
            if (id > nid) {
                l = mid + 1;
            } else if (id < nid) {
                r = mid;
            } else {
                final byte[] inodeBytes = inodes[mid];
                return FsImageProto.INodeSection.INode.parseFrom(inodeBytes);
            }
        }
        return null;
    }

    public int getNumChildren(FsImageProto.INodeSection.INode inode) {
        final long inodeId = inode.getId();
        final long[] children = dirmap.get(inodeId);
        return null != children ? children.length : 0;
    }

    private static final Pattern DOUBLE_SLASH = Pattern.compile("//+");

    static String normalizePath(String path) {
        return DOUBLE_SLASH.matcher(path).replaceAll("/");
    }

}
