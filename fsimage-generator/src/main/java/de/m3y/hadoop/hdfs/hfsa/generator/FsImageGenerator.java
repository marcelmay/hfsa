package de.m3y.hadoop.hdfs.hfsa.generator;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumSet;

import de.m3y.hadoop.hdfs.hfsa.util.IECBinary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/**
 * Generates an FSImage for testing.
 */
public class FsImageGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(FsImageGenerator.class);
    static final String ABC = "abcdefghijklmnopqrstuvwxyz";
    private static final DecimalFormat DF = new DecimalFormat("0.00");

    public static void main(String[] args) throws IOException {
        /* How deep aka directory levels  */
        final int maxDirDepth = Math.min(5, ABC.length());
        /* How many directory children per depth level */
        final int maxDirWidth = Math.min(2, ABC.length());
        /* "a...z".length * factor = number of files per directory generated */
        final int filesPerDirectoryFactor = 10;
        final boolean enableDefaultCompression = Boolean.valueOf(System.getProperty(
                DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, Boolean.toString(DFSConfigKeys.DFS_IMAGE_COMPRESS_DEFAULT)));

        /*
         * #dirs = (1-maxWidth**maxDepth)/(1-maxWidth) * 26
         * #files = #dirs * 26 * filesPerDirectoryFactor
         *
         * 5/6/10 => 20306 directories and 5279560 files, ~270MiB
         * 5/3/10 => 3146 directories and 817960 files, ~40MiB
         * 5/2/10 => 3146 directories and 817960 files, ~40MiB
         */
        LOG.info("Max depth = {}, max width = {}, files-factor = {}",
                maxDirDepth, maxDirWidth, filesPerDirectoryFactor);
        int eDirs = ABC.length() * (1 - (int) (Math.rint(Math.pow(maxDirWidth, maxDirDepth)))) / (1 - maxDirWidth);
        final int numFiles = eDirs * ABC.length() * filesPerDirectoryFactor;
        LOG.info("Generates {} dirs (depth up to {}) and {} files",
                eDirs, maxDirDepth, numFiles);

        HdfsConfiguration conf = new HdfsConfiguration();
        LOG.info("Enabling compression: {}", enableDefaultCompression);
        conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, enableDefaultCompression);


        try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
            long dirCounter = 0;
            long fileCounter = 0;

            try (DistributedFileSystem dfs = cluster.getFileSystem()) {
                final int fileBufferSize = dfs.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, 1024);
                final short fileReplication = dfs.getDefaultReplication();
                final long fileBlockSize = dfs.getDefaultBlockSize();
                final FsPermission fsPermission = FsCreateModes.applyUMask(FsPermission.getFileDefault(),
                        FsPermission.getUMask(dfs.getConf()));
                final EnumSet<CreateFlag> createFlags = EnumSet.of(CreateFlag.CREATE);


                Deque<String> stack = new ArrayDeque<>();
                ABC.chars().forEach(c -> stack.push("/" + (char) c));

                long timestamp = System.currentTimeMillis();
                while (!stack.isEmpty()) {
                    String pathName = stack.pop();
                    final Path path = new Path(pathName);

                    if (dirCounter > 0 && dirCounter % 100 == 0) {
                        LOG.debug("Current path: {}", path);
                        long ts2 = System.currentTimeMillis();
                        final double filesCompletionRate = (double) fileCounter / (double) numFiles * 100.0;
                        LOG.info("Progress: {} directories and {} files ({}%) ... [{}ms]", dirCounter, fileCounter,
                                DF.format(filesCompletionRate), (ts2 - timestamp));
                        timestamp = ts2;
                    }

                    // Create directory
                    dfs.mkdirs(path);
                    dirCounter++;

                    // Fill directory with some files
                    for (int i = 0; i < ABC.length(); i++) {
                        for (int c = 0; c < filesPerDirectoryFactor; c++) {
                            final Path filePath = new Path(path, "" + ABC.charAt(i) + "_" + c);
                            dfs.create(filePath, fsPermission, createFlags, fileBufferSize, fileReplication,
                                    fileBlockSize, null, null).close();
                            fileCounter++;
                        }
                    }

                    final String name = path.getName();
                    if (name.length() < maxDirDepth) {
                        for (int i = 0; i < maxDirWidth /* Limit width for depth */; i++) {
                            stack.push(pathName + "/" + name + ABC.charAt(i));
                        }
                    }
                }

                // Dump FSImage and make backup of file
                dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
                dfs.saveNamespace();
                final FSImage fsImage = cluster.getNameNode().getFSImage();
                final File highestFsImageName = fsImage.getStorage().getHighestFsImageName();
                File newFsImage = new File("fsimage.img");
                if (newFsImage.exists()) {
                    Files.delete(newFsImage.toPath());
                }
                highestFsImageName.renameTo(newFsImage);

                LOG.info("Created new FSImage containing meta data for {} directories and {} files (size={})",
                        dirCounter, fileCounter, IECBinary.format(newFsImage.length()));
                LOG.info("FSImage path : {}", newFsImage.getAbsolutePath());
            }
        }
    }
}
