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
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.LimitInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

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
                        throw new RuntimeException(e);
                    }
                }
            };

    public int getNumChildren(FsImageProto.INodeSection.INode inode) {
        return dirmap.containsKey(inode.getId()) ? dirmap.get(inode.getId()).length : 0;
    }

    static class SectionComparator implements Comparator<FsImageProto.FileSummary.Section> {
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
            Collections.sort(sections, new SectionComparator());

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
        LOG.info("Loading inode directory section");
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
        LOG.info("Loaded " + counter + " directories");
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
        LOG.info("Loaded " + counter + " inode references");
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
        Arrays.parallelSort(inodes, INODE_BYTES_COMPARATOR);
        LOG.debug("Finished sorting inodes");
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


    List<Map<String, Object>> getFileStatusList(String path)
            throws IOException {
        long id = lookup(path);
        FsImageProto.INodeSection.INode inode = fromINodeId(id);
        if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
            if (!dirmap.containsKey(id)) {
                // if the directory is empty, return empty list
                return Collections.emptyList();
            }
            long[] children = dirmap.get(id);
            List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
            for (long cid : children) {
                list.add(getFileStatus(fromINodeId(cid), true));
            }
            return list;
        } else {
            return Collections.singletonList(getFileStatus(inode, false));
        }
    }

    /**
     * Traverses FS tree, starting at root ("/").
     *
     * @param visitor the visitor.
     * @throws IOException on error.
     */
    public void visit(FsVisitor visitor) throws IOException {
        final long rootNodeId = lookup("/");
        visit(visitor, rootNodeId);
    }

    /**
     * Traverses FS tree, using Java parallel stream.
     *
     * @param visitor the visitor.
     * @throws IOException on error.
     */
    public void visitParallel(FsVisitor visitor) throws IOException {
        final long rootNodeId = lookup("/");
        FsImageProto.INodeSection.INode rootNode = fromINodeId(rootNodeId);
        visitor.onDirectory(rootNode);
        if (dirmap.containsKey(rootNodeId)) {
            long[] children = dirmap.get(rootNodeId);
            List<Long> dirs = new ArrayList<>();
            for (long cid : children) {
                final FsImageProto.INodeSection.INode inode = fromINodeId(cid);
                if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
                    dirs.add(cid);
                } else {
                    visit(visitor, cid);
                }
            }
            dirs.parallelStream().forEach(nodeId -> {
                try {
                    visit(visitor, nodeId);
                } catch (IOException e) {
                    LOG.error("Can not traverse "+nodeId, e);
                }
            });
        }
    }

    void visit(FsVisitor visitor, long nodeId) throws IOException {
        FsImageProto.INodeSection.INode inode = fromINodeId(nodeId);
        if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
            visitor.onDirectory(inode);
            if (dirmap.containsKey(nodeId)) {
                long[] children = dirmap.get(nodeId);
                for (long cid : children) {
                    visit(visitor, cid);
                }
            }
        } else if (inode.getType() == FsImageProto.INodeSection.INode.Type.FILE) {
            visitor.onFile(inode);
        } else if (inode.getType() == FsImageProto.INodeSection.INode.Type.SYMLINK) {
            visitor.onSymLink(inode);
        } else {
            // Should not happen
            throw new IllegalStateException("Unsupported inode type " + inode.getType() + " for " + inode);
        }
    }

    /**
     * Return the JSON formatted ACL status of the specified file.
     *
     * @param path a path specifies a file
     * @return JSON formatted AclStatus
     * @throws IOException if failed to serialize fileStatus to JSON.
     */
    String getAclStatus(String path) throws IOException {
        PermissionStatus p = getPermissionStatus(path);
        List<AclEntry> aclEntryList = getAclEntryList(path);
        FsPermission permission = p.getPermission();
        AclStatus.Builder builder = new AclStatus.Builder();
        builder.owner(p.getUserName()).group(p.getGroupName())
                .addEntries(aclEntryList).setPermission(permission)
                .stickyBit(permission.getStickyBit());
        AclStatus aclStatus = builder.build();
        return JsonUtil.toJsonString(aclStatus);
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
                return new ArrayList<AclEntry>();
            }
        }
    }

    private PermissionStatus getPermissionStatus(String path) throws IOException {
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

    public Map<String, Object> getFileStatus
            (FsImageProto.INodeSection.INode inode, boolean printSuffix) {
        Map<String, Object> map = Maps.newHashMap();
        switch (inode.getType()) {
            case FILE: {
                FsImageProto.INodeSection.INodeFile f = inode.getFile();
                PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
                        f.getPermission(), stringTable);
                map.put("accessTime", f.getAccessTime());
                map.put("blockSize", f.getPreferredBlockSize());
                map.put("group", p.getGroupName());
                map.put("length", getFileSize(f));
                map.put("modificationTime", f.getModificationTime());
                map.put("owner", p.getUserName());
                map.put("pathSuffix",
                        printSuffix ? inode.getName().toStringUtf8() : "");
                map.put("permission", toString(p.getPermission()));
                map.put("replication", f.getReplication());
                map.put("type", inode.getType());
                map.put("fileId", inode.getId());
                map.put("childrenNum", 0);
                return map;
            }
            case DIRECTORY: {
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
                        d.getPermission(), stringTable);
                map.put("accessTime", 0);
                map.put("blockSize", 0);
                map.put("group", p.getGroupName());
                map.put("length", 0);
                map.put("modificationTime", d.getModificationTime());
                map.put("owner", p.getUserName());
                map.put("pathSuffix",
                        printSuffix ? inode.getName().toStringUtf8() : "");
                map.put("permission", toString(p.getPermission()));
                map.put("replication", 0);
                map.put("type", inode.getType());
                map.put("fileId", inode.getId());
                map.put("childrenNum", dirmap.containsKey(inode.getId()) ?
                        dirmap.get(inode.getId()).length : 0);
                return map;
            }
            case SYMLINK: {
                FsImageProto.INodeSection.INodeSymlink d = inode.getSymlink();
                PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
                        d.getPermission(), stringTable);
                map.put("accessTime", d.getAccessTime());
                map.put("blockSize", 0);
                map.put("group", p.getGroupName());
                map.put("length", 0);
                map.put("modificationTime", d.getModificationTime());
                map.put("owner", p.getUserName());
                map.put("pathSuffix",
                        printSuffix ? inode.getName().toStringUtf8() : "");
                map.put("permission", toString(p.getPermission()));
                map.put("replication", 0);
                map.put("type", inode.getType());
                map.put("symlink", d.getTarget().toStringUtf8());
                map.put("fileId", inode.getId());
                map.put("childrenNum", 0);
                return map;
            }
            default:
                return null;
        }
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

    private FsImageProto.INodeSection.INode fromINodeId(final long id)
            throws IOException {
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
}
