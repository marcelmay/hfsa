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
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.primitives.ImmutableLongArray;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.LimitInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.namenode.SerialNumberManager.StringTable;
import static org.apache.hadoop.hdfs.server.namenode.SerialNumberManager.newStringTable;

/**
 * FSImageLoader loads fsimage and provide methods to return
 * file status of the namespace of the fsimage.
 * <p>
 * Note: This class is based on the original FSImageLoader from Hadoop:
 * https://github.com/apache/hadoop/blob/master/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/FSImageLoader.java
 * <p>
 * An introduction to FSImage design:
 * https://jira.apache.org/jira/browse/HDFS-5698
 */
public class FSImageLoader {
    private static final Logger LOG = LoggerFactory.getLogger(FSImageLoader.class);
    private static final BlockStoragePolicySuite BLOCK_STORAGE_POLICY_SUITE = BlockStoragePolicySuite.createDefaultSuite();

    public static final String ROOT_PATH = "/";

    private final StringTable stringTable;
    // byte representation of inodes, sorted by id
    private final INodesRepository inodes;
    private final Long2ObjectLinkedOpenHashMap<long[]> dirmap;

    /**
     * Manages inodes.
     */
    interface INodesRepository {
        /**
         * Gets an inode by its identifier.
         *
         * @param inodeId the inode identifier.
         * @return the inode
         * @throws InvalidProtocolBufferException if protobuf deserialization fails
         */
        FsImageProto.INodeSection.INode getInode(long inodeId) throws InvalidProtocolBufferException;

        /**
         * Gets the number of inodes in this repository.
         *
         * @return the number of inodes.
         */
        int getSize();
    }

    /**
     * Implementation of INode repository using array of bytes.
     */
    static class PrimitiveArrayINodesRepository implements INodesRepository {
        private static final Comparator<byte[]> INODE_BYTES_COMPARATOR =
                Comparator.comparingLong(PrimitiveArrayINodesRepository::extractNodeId);

        private final byte[][] inodes;
        // inodesIdxToIdCache contains the INode ID, to avoid redundant parsing when using fromINodeId
        private final long[] inodesIdxToIdCache;

        PrimitiveArrayINodesRepository(byte[][] buf, long[] inodeOffsets) {
            inodes = buf;
            this.inodesIdxToIdCache = inodeOffsets;
        }

        static PrimitiveArrayINodesRepository create(FsImageProto.INodeSection s, InputStream in, long length) throws IOException {
            long start = System.currentTimeMillis();
            final byte[][] inodes = new byte[(int) s.getNumInodes()][];
            for (int i = 0; i < s.getNumInodes(); ++i) {
                int size = CodedInputStream.readRawVarint32(in.read(), in);
                byte[] bytes = new byte[size];
                IOUtils.readFully(in, bytes, 0, size);
                inodes[i] = bytes;
            }
            LOG.info("Loaded {} inodes [{}ms] of length {} bytes",
                    s.getNumInodes(), System.currentTimeMillis() - start, length);
            start = System.currentTimeMillis();
            Arrays.parallelSort(inodes, INODE_BYTES_COMPARATOR);
            LOG.info("Sorted {} inodes [{}ms]", inodes.length, System.currentTimeMillis() - start);
            return new PrimitiveArrayINodesRepository(inodes, computeInodesIdxToIdCache(inodes));
        }

        private static long[] computeInodesIdxToIdCache(byte[][] buf) {
            long start = System.currentTimeMillis();
            long[] cache = new long[buf.length];
            // Compute inode idx to inode id cache
            for (int i = 0; i < cache.length; i++) {
                cache[i] = extractNodeId(buf[i]);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Computed inodes idx to id cache in {}ms", System.currentTimeMillis() - start);
            }
            return cache;
        }

        private byte[] getInodeAsBytes(final long inodeId) {
            // Binary search over sorted node id array
            int l = 0;
            int r = inodes.length - 1;
            while (l <= r) {
                int mid = (l + r) >>> 1;
                long currentInodeId = inodesIdxToIdCache[mid];

                if (currentInodeId < inodeId) {
                    l = mid + 1;
                } else if (currentInodeId > inodeId) {
                    r = mid - 1;
                } else {
                    return inodes[mid];
                }
            }
            throw new IllegalArgumentException("Can not find inode by id " + inodeId);
        }

        @Override
        public FsImageProto.INodeSection.INode getInode(long inodeId) throws InvalidProtocolBufferException {
            return FsImageProto.INodeSection.INode.parseFrom(getInodeAsBytes(inodeId));
        }

        @Override
        public int getSize() {
            return inodes.length;
        }

        private static long extractNodeId(byte[] buf) {
            // Pretty much of a hack, as Protobuf 2.5 does not partial parsing
            // In a micro benchmark, it is several times(!) faster than
            // FsImageProto.INodeSection.INode.parseFrom(o2).getId()
            // - obiously, there are less instances created and less unmarshalling involed.

            // INode wire format:
            // tag 8
            // enum
            // tag 16
            // id (long)

            // Even more optimized, no direct object creation such as CodedInputStream:
            // Extracted from CodedInputStream.readRawVarint64()
            int bufferPos = 3; /* tag + enum + tag */
            int shift = 0;
            long result = 0;
            while (shift < 64) {
                final byte b = buf[bufferPos++];
                result |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
                shift += 7;
            }
            throw new IllegalArgumentException("Malformed Varint at pos 3 : ["
                    + buf[3] + "," + buf[4] + "," + buf[5] + "," + buf[6] + "]");
        }
    }


    private FSImageLoader(StringTable stringTable, INodesRepository inodes,
                          Long2ObjectLinkedOpenHashMap<long[]> dirmap) {
        this.stringTable = stringTable;
        this.inodes = inodes;
        this.dirmap = dirmap;
    }


    private static FileSummary.Section findSectionByName(
            List<FileSummary.Section> sectionList, SectionName sectionName) {
        // Section list is ~ 10 elements, so no map for efficient lookup required
        for (FileSummary.Section section : sectionList) {
            if (sectionName.name().equals(section.getName())) {
                return section;
            }
        }
        throw new IllegalStateException("No such section of name " + sectionName + " found in " +
                sectionList.stream().map(FileSummary.Section::getName).collect(Collectors.joining(", ")));
    }

    @FunctionalInterface
    interface IOFunction<R> {
        R apply(InputStream t, long length) throws IOException;
    }

    private static <T> T loadSection(FileInputStream fin,
                                     String codec,
                                     FileSummary.Section section,
                                     IOFunction<T> f) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading fsimage section {} of {} bytes", section.getName(), section.getLength());
        }
        long startTime = System.currentTimeMillis();
        try {
            FileChannel fc = fin.getChannel();
            fc.position(section.getOffset());

            // Min 8 KiB, max 512 KiB buffer
            final int bufferSize = Math.max(
                    (int) Math.min(section.getLength(), 1024L * 1024L /* 1024KiB */),
                    8 * 1024 /* 8KiB */);
            InputStream is = FSImageUtil.wrapInputStreamForCompression(null, codec,
                    new FastBufferedInputStream(new LimitInputStream(fin, section.getLength()), bufferSize));

            final T apply = f.apply(is, section.getLength());
            LOG.info("Loaded fsimage section {} in {}ms", section.getName(), System.currentTimeMillis() - startTime);
            return apply;
        } catch (IOException ex) {
            throw new IllegalStateException("Can not load fsimage section " + section.getName(), ex);
        }
    }

    /**
     * Load fsimage into the memory.
     *
     * @param file the filepath of the fsimage to load.
     * @return FSImageLoader
     * @throws IOException if failed to load fsimage.
     */
    public static FSImageLoader load(RandomAccessFile file) throws IOException {
        if (!FSImageUtil.checkFileFormat(file)) {
            throw new IOException("Unrecognized FSImage format");
        }

        FileSummary summary = FSImageUtil.loadSummary(file);
        String codec = summary.getCodec();
        try (FileInputStream fin = new FileInputStream(file.getFD())) {
            // Section list only
            final List<FileSummary.Section> sectionsList = summary.getSectionsList();

            FileSummary.Section sectionStringTable = findSectionByName(sectionsList, SectionName.STRING_TABLE);
            StringTable stringTable = loadSection(fin, codec, sectionStringTable, FSImageLoader::loadStringTable);

            FileSummary.Section sectionInodeRef = findSectionByName(sectionsList, SectionName.INODE_REFERENCE);
            ImmutableLongArray refIdList = loadSection(fin, codec, sectionInodeRef, FSImageLoader::loadINodeReferenceSection);

            FileSummary.Section sectionInode = findSectionByName(sectionsList, SectionName.INODE);
            INodesRepository inodes = loadSection(fin, codec, sectionInode, FSImageLoader::loadINodeSection); // SLOW!!!

            FileSummary.Section sectionInodeDir = findSectionByName(sectionsList, SectionName.INODE_DIR);
            Long2ObjectLinkedOpenHashMap<long[]> dirmap = loadSection(fin, codec, sectionInodeDir,
                    (InputStream is, long length) -> FSImageLoader.loadINodeDirectorySection(is, refIdList)); // SLOW!!!

            return new FSImageLoader(stringTable, inodes, dirmap);
        }
    }

    private static Long2ObjectLinkedOpenHashMap<long[]> loadINodeDirectorySection(InputStream in, ImmutableLongArray refIdList)
            throws IOException {
        Long2ObjectLinkedOpenHashMap<long[]> dirs = new Long2ObjectLinkedOpenHashMap<>(512 * 1024 /* 512K */);
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
        LOG.info("Loaded {} directories", dirs.size());
        return dirs;
    }

    private static ImmutableLongArray loadINodeReferenceSection(InputStream in, long length) throws IOException {
        ImmutableLongArray.Builder builder = ImmutableLongArray.builder();
        while (true) {
            FsImageProto.INodeReferenceSection.INodeReference e =
                    FsImageProto.INodeReferenceSection.INodeReference
                            .parseDelimitedFrom(in);
            if (e == null) {
                break;
            }
            builder.add(e.getReferredId());
        }
        final ImmutableLongArray array = builder.build();
        LOG.info("Loaded {} inode references of length {} bytes", array.length(), length);
        return array;
    }

    // Slow
    private static INodesRepository loadINodeSection(InputStream in, long length) throws IOException {
        FsImageProto.INodeSection s = FsImageProto.INodeSection
                .parseDelimitedFrom(in);
        return PrimitiveArrayINodesRepository.create(s, in, length);
    }

    static StringTable loadStringTable(InputStream in, long length) throws IOException {
        FsImageProto.StringTableSection s = FsImageProto.StringTableSection.parseDelimitedFrom(in);
        StringTable stringTable =
                newStringTable(s.getNumEntry(), s.getMaskBits());
        for (int i = 0; i < s.getNumEntry(); ++i) {
            FsImageProto.StringTableSection.Entry e = FsImageProto
                    .StringTableSection.Entry.parseDelimitedFrom(in);
            stringTable.put(e.getId(), e.getStr());
        }
        LOG.info("Loaded {} strings into string table of length {} bytes", s.getNumEntry(), length);
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
                final FsImageProto.INodeSection.INode inode = inodes.getInode(cid);
                if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
                    dirs.add(inode);
                } else {
                    visit(visitor, inode, path);
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
        final FsImageProto.INodeSection.INode inode = inodes.getInode(nodeId);
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
            final FsImageProto.INodeSection.INode inode = inodes.getInode(cid);
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
        Preconditions.checkArgument(path.startsWith(ROOT_PATH),
                "Expected path <" + path + "> to start with " + ROOT_PATH);
        String normalizedPath = normalizePath(path);
        long id = INodeId.ROOT_INODE_ID;
        // Root node?
        if (ROOT_PATH.equals(normalizedPath)) {
            return inodes.getInode(id);
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
                node = inodes.getInode(cid);
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
            return false;
        }
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
            final FsImageProto.INodeSection.INode inode = inodes.getInode(cid);
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
                return Collections.emptyList();
            }
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
     * Returns the INode Id of the specified path, or if not found throws FileNotFoundException.
     *
     * @param path the path.
     * @return the inode id.
     */
    private long lookup(String path) throws IOException {
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
     * Computes the file size.
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

    public static String toString(FsPermission permission) {
        return String.format("%o", permission.toShort());
    }

    public static BlockStoragePolicy getBlockStoragePolicy(FsImageProto.INodeSection.INodeFile iNodeFile) {
        if (iNodeFile.hasStoragePolicyID()) {
            byte policyId = (byte) iNodeFile.getStoragePolicyID();
            return BLOCK_STORAGE_POLICY_SUITE.getPolicy(policyId);
        }
        return BLOCK_STORAGE_POLICY_SUITE.getDefaultPolicy();
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
