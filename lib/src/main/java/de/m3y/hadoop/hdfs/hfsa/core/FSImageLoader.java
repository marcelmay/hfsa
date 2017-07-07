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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
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

    private final String[] stringTable;
    // byte representation of inodes, sorted by id
    private final byte[][] inodes;
    private final Map<Long, long[]> dirmap;
    private static final Comparator<byte[]> INODE_BYTES_COMPARATOR = new
            Comparator<byte[]>() {
                @Override
                public int compare(byte[] o1, byte[] o2) {
                    try {
                        final FsImageProto.INodeSection.INode l = FsImageProto.INodeSection
                                .INode.parseFrom(o1);
                        final FsImageProto.INodeSection.INode r = FsImageProto.INodeSection
                                .INode.parseFrom(o2);
                        if (l.getId() < r.getId()) {
                            return -1;
                        } else if (l.getId() > r.getId()) {
                            return 1;
                        } else {
                            return 0;
                        }
                    } catch (InvalidProtocolBufferException e) {
                        throw new IllegalArgumentException(e);
                    }
                }
            };

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

            ArrayList<FsImageProto.FileSummary.Section> sections =
                    Lists.newArrayList(summary.getSectionsList());
            sections.sort(SECTION_COMPARATOR);

            for (FsImageProto.FileSummary.Section s : sections) {
                fin.getChannel().position(s.getOffset());
                InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
                        summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                                fin, s.getLength())));

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Loading section " + s.getName() + " length: " + s.getLength());
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
                        break;
                }
            }
            return new FSImageLoader(stringTable, inodes, dirmap);
        }
    }

    private static Map<Long, long[]> loadINodeDirectorySection
            (InputStream in, List<Long> refIdList)
            throws IOException {
        LOG.info("Loading inode directory section ...");
        long start = System.currentTimeMillis();
        Map<Long, long[]> dirs = Maps.newHashMap();
        long counter = 0;
        while (true) {
            FsImageProto.INodeDirectorySection.DirEntry e =
                    FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
            // note that in is a LimitedInputStream
            if (e == null) {
                break;
            }
            ++counter;

            long[] l = new long[e.getChildrenCount() + e.getRefChildrenCount()];
            for (int i = 0; i < e.getChildrenCount(); ++i) {
                l[i] = e.getChildren(i);
            }
            for (int i = e.getChildrenCount(); i < l.length; i++) {
                int refId = e.getRefChildren(i - e.getChildrenCount());
                l[i] = refIdList.get(refId);
            }
            dirs.put(e.getParent(), l);
        }
        LOG.info("Loaded " + counter + " directories [" + (System.currentTimeMillis() - start) + "ms]");
        return dirs;
    }

    private static ImmutableList<Long> loadINodeReferenceSection(InputStream in)
            throws IOException {
        LOG.info("Loading inode references");
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
        LOG.info("Loaded {} inode references", counter);
        return builder.build();
    }

    private static byte[][] loadINodeSection(InputStream in)
            throws IOException {
        FsImageProto.INodeSection s = FsImageProto.INodeSection
                .parseDelimitedFrom(in);
        LOG.info("Loading " + s.getNumInodes() + " inodes.");
        final byte[][] inodes = new byte[(int) s.getNumInodes()][];

        for (int i = 0; i < s.getNumInodes(); ++i) {
            int size = CodedInputStream.readRawVarint32(in.read(), in);
            byte[] bytes = new byte[size];
            IOUtils.readFully(in, bytes, 0, size);
            inodes[i] = bytes;
        }
        LOG.debug("Sorting inodes");
        long start = System.currentTimeMillis();
        Arrays.parallelSort(inodes, INODE_BYTES_COMPARATOR);
        LOG.info("Finished sorting inodes [{}ms]", System.currentTimeMillis() - start);
        return inodes;
    }

    static String[] loadStringTable(InputStream in) throws
            IOException {
        FsImageProto.StringTableSection s = FsImageProto.StringTableSection
                .parseDelimitedFrom(in);
        LOG.info("Loading " + s.getNumEntry() + " strings");
        String[] stringTable = new String[s.getNumEntry() + 1];
        for (int i = 0; i < s.getNumEntry(); ++i) {
            FsImageProto.StringTableSection.Entry e = FsImageProto
                    .StringTableSection.Entry.parseDelimitedFrom(in);
            stringTable[e.getId()] = e.getStr();
        }
        return stringTable;
    }

    /**
     * Traverses FS tree, starting at root ("/").
     *
     * @param visitor the visitor.
     * @throws IOException on error.
     */
    public void visit(FsVisitor visitor) throws IOException {
        visit(visitor, "/");
    }

    /**
     * Traverses FS tree, starting at given directory path
     *
     * @param visitor the visitor.
     * @param path    the directory path to start with
     * @throws IOException on error.
     */
    public void visit(FsVisitor visitor, String path) throws IOException {
        final long nodeId = lookup(path);
        if (dirmap.containsKey(nodeId)) {
            long[] children = dirmap.get(nodeId);
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
        visitParallel(visitor, "/");
    }

    /**
     * Traverses FS tree, using Java parallel stream.
     *
     * @param visitor the visitor.
     * @param path    the directory path to start with
     * @throws IOException on error.
     */
    public void visitParallel(FsVisitor visitor, String path) throws IOException {
        final long rootNodeId = lookup(path);
        FsImageProto.INodeSection.INode rootNode = fromINodeId(rootNodeId);
        visitor.onDirectory(rootNode, path);
        if (dirmap.containsKey(rootNodeId)) {
            long[] children = dirmap.get(rootNodeId);
            List<Long> dirs = new ArrayList<>();
            for (long cid : children) {
                final FsImageProto.INodeSection.INode inode = fromINodeId(cid);
                if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
                    dirs.add(cid);
                } else {
                    visit(visitor, cid, ("/".equals(path) ? path : path + '/') + inode.getName().toStringUtf8());
                }
            }
            dirs.parallelStream().forEach(nodeId -> {
                try {
                    visit(visitor, nodeId, path);
                } catch (IOException e) {
                    LOG.error("Can not traverse " + nodeId, e);
                }
            });
        }
    }

    void visit(FsVisitor visitor, long nodeId, String path) throws IOException {
        FsImageProto.INodeSection.INode inode = fromINodeId(nodeId);
        if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
            visitor.onDirectory(inode, path);
            if (dirmap.containsKey(nodeId)) {
                String newPath;
                if ("/".equals(path)) {
                    newPath = path + inode.getName().toStringUtf8();
                } else {
                    newPath = path + '/' + inode.getName().toStringUtf8();
                }
                long[] children = dirmap.get(nodeId);
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
     * Gets the child directory paths for given path.
     *
     * @param path the parent directory path.
     * @return the list of child directory paths.
     * @throws IOException on error.
     */
    public List<String> getChildPaths(String path) throws IOException {
        final long rootNodeId = lookup(path);
        if (!dirmap.containsKey(rootNodeId)) {
            throw new NoSuchElementException("No node found for path " + path);
        }
        long[] children = dirmap.get(rootNodeId);
        List<String> childPaths = new ArrayList<>();
        for (long cid : children) {
            final FsImageProto.INodeSection.INode inode = fromINodeId(cid);
            if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
                childPaths.add(("/".equals(path) ? path : path + '/') + inode.getName().toStringUtf8());
            }
        }
        return childPaths;
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
        long id = lookup(path);
        FsImageProto.INodeSection.INode inode = fromINodeId(id);
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
        long id = lookup(path);
        FsImageProto.INodeSection.INode inode = fromINodeId(id);
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
                return null;
            }
        }
    }

    /**
     * Return the INodeId of the specified path.
     *
     * @param path the path.
     * @return the inode id.
     */
    private long lookup(String path) throws IOException {
        Preconditions.checkArgument(path.startsWith("/"));
        long id = INodeId.ROOT_INODE_ID;
        for (int offset = 0, next; offset < path.length(); offset = next) {
            next = path.indexOf('/', offset + 1);
            if (next == -1) {
                next = path.length();
            }
            if (offset + 1 > next) {
                break;
            }

            final String component = path.substring(offset + 1, next);

            if (component.isEmpty()) {
                continue;
            }

            final long[] children = dirmap.get(id);
            if (children == null) {
                throw new FileNotFoundException(path);
            }

            boolean found = false;
            for (long cid : children) {
                FsImageProto.INodeSection.INode child = fromINodeId(cid);
                if (component.equals(child.getName().toStringUtf8())) {
                    found = true;
                    id = child.getId();
                    break;
                }
            }
            if (!found) {
                throw new FileNotFoundException(path);
            }
        }
        return id;
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
        int l = 0, r = inodes.length;
        while (l < r) {
            int mid = l + (r - l) / 2;
            FsImageProto.INodeSection.INode n = FsImageProto.INodeSection.INode
                    .parseFrom(inodes[mid]);
            long nid = n.getId();
            if (id > nid) {
                l = mid + 1;
            } else if (id < nid) {
                r = mid;
            } else {
                return n;
            }
        }
        return null;
    }

    public int getNumChildren(FsImageProto.INodeSection.INode inode) {
        return dirmap.containsKey(inode.getId()) ? dirmap.get(inode.getId()).length : 0;
    }
}
