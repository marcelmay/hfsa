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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.primitives.ImmutableLongArray;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.thirdparty.protobuf.CodedInputStream;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.thirdparty.protobuf.Parser;
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
 * <p>
 * Usage:
 * <code>
 * new FSImageLoader.Builder()
 * .parallel()
 * .build()
 * .load(new RandomAccessFile("src/test/resources/fsi_small_h3_2.img", "r"));
 * </code>
 */
public class FsImageLoader {
    private static final Logger LOG = LoggerFactory.getLogger(FsImageLoader.class);
    private final Builder.LoadingStrategy loadingStrategy;

    public FsImageLoader(Builder.LoadingStrategy loadingStrategy) {
        this.loadingStrategy = loadingStrategy;
    }

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
        INode getInode(long inodeId) throws IOException;

        /**
         * Gets the number of inodes in this repository.
         *
         * @return the number of inodes.
         */
        int getSize();
    }

    interface INodesRepositoryBuilder {
        INodesRepository build(FsImageProto.INodeSection s, InputStream in, long length) throws IOException;
    }

    /**
     * Implementation of INode repository using array of bytes.
     */
    static class PrimitiveArrayINodesRepository implements INodesRepository {
        private static final Parser<INode> INODE_PARSER = INode.parser();
        private static final Comparator<byte[]> INODE_BYTES_COMPARATOR =
                Comparator.comparingLong(PrimitiveArrayINodesRepository::extractNodeId);
        // byte representation of inodes, sorted by id
        private final byte[][] inodes;
        // inodesIdxToIdCache contains the INode ID, to avoid redundant parsing when using fromINodeId
        private final long[] inodesIdxToIdCache;
        private final INode rootInode;

        PrimitiveArrayINodesRepository(byte[][] buf, long[] inodeOffsets) throws InvalidProtocolBufferException {
            inodes = buf;
            this.inodesIdxToIdCache = inodeOffsets;
            rootInode = INODE_PARSER.parseFrom(getInodeAsBytes(INodeId.ROOT_INODE_ID));
        }

        static class Builder implements INodesRepositoryBuilder {
            @Override
            public INodesRepository build(FsImageProto.INodeSection s, InputStream in, long length) throws IOException {
                long start = System.currentTimeMillis();
                final byte[][] inodes = new byte[(int) s.getNumInodes()][];
                for (int i = 0; i < s.getNumInodes(); ++i) {
                    int size = CodedInputStream.readRawVarint32(in.read(), in);
                    byte[] bytes = new byte[size];
                    IOUtils.readFully(in, bytes, 0, size);
                    inodes[i] = bytes;
                }
                LOG.debug("Loaded {} inodes [{}ms] of length {} bytes",
                        s.getNumInodes(), System.currentTimeMillis() - start, length);
                start = System.currentTimeMillis();
                sortINodes(inodes);
                LOG.debug("Sorted {} inodes [{}ms]", inodes.length, System.currentTimeMillis() - start);
                return new PrimitiveArrayINodesRepository(inodes, computeInodesIdxToIdCache(inodes));
            }

            protected void sortINodes(byte[][] inodes) {
                // TODO: Cache INodes?
                Arrays.sort(inodes, INODE_BYTES_COMPARATOR);
            }

            protected long[] computeInodesIdxToIdCache(byte[][] buf) {
                long start = System.currentTimeMillis();
                long[] cache = new long[buf.length];
                // Compute inode idx to inode id cache
                for (int i = 0; i < cache.length; i++) { // TODO: Parallel?
                    cache[i] = extractNodeId(buf[i]);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Computed inodes idx to id cache[len={}] in {}ms",
                            cache.length, System.currentTimeMillis() - start);
                }
                return cache;
            }
        }

        static class ParallelBuilder extends Builder {
            @Override
            protected void sortINodes(byte[][] inodes) {
                Arrays.parallelSort(inodes, INODE_BYTES_COMPARATOR);
            }
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
        public INode getInode(long inodeId) throws IOException {
            if (INodeId.ROOT_INODE_ID == inodeId) {
                return rootInode;
            }
            return INODE_PARSER.parseFrom(getInodeAsBytes(inodeId));
        }

        @Override
        public int getSize() {
            return inodes.length;
        }

        private static long extractNodeId(byte[] buf) {
            // Pretty much of a hack, as Protobuf 2.5 does not partial parsing
            // In a micro benchmark, it is several times(!) faster than
            // FsImageProto.INodeSection.INode.parseFrom(o2).getId()
            // - obviously, there are less instances created and less unmarshalling involved.

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

    private FileSummary.Section findSectionByName(
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
    private interface IOFunction<R> {
        R apply(InputStream t, long length) throws IOException;
    }

    private <T> T loadSection(FileInputStream fin,
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

            // Min 8 KiB, max 1024 KiB buffer
            final int bufferSize = Math.max(
                    (int) Math.min(section.getLength(), 1024L * 1024L /* 1024KiB */),
                    8 * 1024 /* 8KiB */);
            InputStream is = FSImageUtil.wrapInputStreamForCompression(new Configuration(), codec,
                    new FastBufferedInputStream(new LimitInputStream(fin, section.getLength()), bufferSize));

            final T apply = f.apply(is, section.getLength());
            LOG.debug("Loaded fsimage section {} in {}ms", section.getName(), System.currentTimeMillis() - startTime);
            return apply;
        } catch (Throwable ex) { // Can be IOException or NoClassDefFoundError
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
    public FsImageData load(RandomAccessFile file) throws IOException {
        if (!FSImageUtil.checkFileFormat(file)) {
            throw new IOException("Unrecognized FSImage format (no magic header?)");
        }

        FileSummary summary = FSImageUtil.loadSummary(file);
        String codec = summary.getCodec();
        try (FileInputStream fin = new FileInputStream(file.getFD())) {
            // Section list only
            final List<FileSummary.Section> sectionsList = summary.getSectionsList();

            FileSummary.Section sectionStringTable = findSectionByName(sectionsList, SectionName.STRING_TABLE);
            StringTable stringTable = loadSection(fin, codec, sectionStringTable, this::loadStringTable);

            FileSummary.Section sectionInodeRef = findSectionByName(sectionsList, SectionName.INODE_REFERENCE);
            ImmutableLongArray refIdList = loadSection(fin, codec, sectionInodeRef, this::loadINodeReferenceSection);

            FileSummary.Section sectionInode = findSectionByName(sectionsList, SectionName.INODE);
            INodesRepository inodes = loadSection(fin, codec, sectionInode, this::loadINodeSection); // SLOW!!!

            FileSummary.Section sectionInodeDir = findSectionByName(sectionsList, SectionName.INODE_DIR);

            Long2ObjectLinkedOpenHashMap<long[]> dirMap = loadSection(fin, codec, sectionInodeDir,
                    (InputStream is, long length) -> loadINodeDirectorySection(is, refIdList)); // SLOW!!!

            return new FsImageData(stringTable, inodes, dirMap);
        }
    }

    private Long2ObjectLinkedOpenHashMap<long[]> loadINodeDirectorySection(InputStream in, ImmutableLongArray refIdList)
            throws IOException {
        Long2ObjectLinkedOpenHashMap<long[]> dirs = new Long2ObjectLinkedOpenHashMap<>(512 * 1024 /* 512K */);
        dirs.defaultReturnValue(new long[0] /* Empty array, to avoid null */);
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
        LOG.debug("Loaded {} directories", dirs.size());
        return dirs;
    }

    private ImmutableLongArray loadINodeReferenceSection(InputStream in, long length) throws IOException {
        ImmutableLongArray.Builder builder = ImmutableLongArray.builder(2048);
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
        LOG.debug("Loaded {} inode references of size {} bytes", array.length(), length);
        return array;
    }

    // Slow
    private INodesRepository loadINodeSection(InputStream in, long length) throws IOException {
        FsImageProto.INodeSection s = FsImageProto.INodeSection
                .parseDelimitedFrom(in);
        return this.loadingStrategy.createInodeRepositoryBuilder().build(s, in, length);
    }

    StringTable loadStringTable(InputStream in, long length) throws IOException {
        FsImageProto.StringTableSection s = FsImageProto.StringTableSection.parseDelimitedFrom(in);
        StringTable stringTable =
                newStringTable(s.getNumEntry(), s.getMaskBits());
        for (int i = 0; i < s.getNumEntry(); ++i) {
            FsImageProto.StringTableSection.Entry e = FsImageProto
                    .StringTableSection.Entry.parseDelimitedFrom(in);
            stringTable.put(e.getId(), e.getStr());
        }
        LOG.debug("Loaded {} strings into string table of length {} bytes", s.getNumEntry(), length);
        return stringTable;
    }

    public static class Builder {
        private LoadingStrategy loadingStrategy = PrimitiveArrayINodesRepository.Builder::new;

        interface LoadingStrategy {
            INodesRepositoryBuilder createInodeRepositoryBuilder();
        }

        public Builder parallel() {
            this.loadingStrategy = PrimitiveArrayINodesRepository.ParallelBuilder::new;
            return this;
        }

        public FsImageLoader build() {
            return new FsImageLoader(loadingStrategy);
        }
    }
}
