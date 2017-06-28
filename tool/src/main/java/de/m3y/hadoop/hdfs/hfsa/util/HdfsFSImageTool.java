package de.m3y.hadoop.hdfs.hfsa.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;
import picocli.CommandLine;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * HDFS FSImage Tool extracts a summary of HDFS Usage from fsimage.
 */
public class HdfsFSImageTool {
    private static final Logger log = getLogger(HdfsFSImageTool.class);

    abstract static class AbstractStats {
        long sumFiles;
        long sumDirectories;
        long sumBlocks;
        long sumFileSize;
        final SizeBucket fileSizeBuckets = new SizeBucket();
    }

    static class UserStats extends AbstractStats {
        String userName;
    }

    static class GroupStats extends AbstractStats {
        String groupName;
    }

    static class OverallStats extends AbstractStats {
    }

    public static class Options {
        @CommandLine.Option(names = {"-h", "--help"}, help = true,
                description = "Displays this help message and quits.")
        private boolean helpRequested = false;

        @CommandLine.Option(names = {"-s", "--sort"}, help = true,
                description = "Sort by (fs) file size, (fc) file count.")
        private String sort;

        @CommandLine.Parameters(arity = "1", paramLabel = "FILE", description = "FSImage file to process.")
        private File fsImageFile;
    }

    static void doPerform(Options options) throws IOException {
        RandomAccessFile file = new RandomAccessFile(options.fsImageFile, "r");
        final FSImageLoader loader = FSImageLoader.load(file);

        final Map<String, GroupStats> groupStats = new HashMap<>();
        final Map<String, UserStats> userStats = new HashMap<>();
        final OverallStats overallStats = new OverallStats();
        log.info("Visting ...");
        loader.visit(new FsVisitor() {
            @Override
            public void onFile(FsImageProto.INodeSection.INode inode) {
                FsImageProto.INodeSection.INodeFile f = inode.getFile();

                PermissionStatus p = loader.getPermissionStatus(f.getPermission());

                final long fileSize = FSImageLoader.getFileSize(f);
                final long fileBlocks = f.getBlocksCount();
                overallStats.fileSizeBuckets.add(fileSize);
                overallStats.sumBlocks += fileBlocks;
                overallStats.sumFileSize += fileSize;
                overallStats.sumFiles++;

                // Group stats
                final String groupName = p.getGroupName();
                GroupStats groupStat;
                if (groupStats.containsKey(groupName)) {
                    groupStat = groupStats.get(groupName);
                } else {
                    groupStat = new GroupStats();
                    groupStat.groupName = groupName;
                    groupStats.put(groupName, groupStat);
                }
                groupStat.sumFiles++;
                groupStat.sumFileSize += fileSize;
                groupStat.fileSizeBuckets.add(fileSize);
                groupStat.sumBlocks += fileBlocks;

                // User stats
                final String userName = p.getUserName();
                UserStats user;
                if (userStats.containsKey(userName)) {
                    user = userStats.get(userName);
                } else {
                    user = new UserStats();
                    user.userName = userName;
                    userStats.put(userName, user);
                }
                user.sumFiles++;
                user.sumFileSize += fileSize;
                user.fileSizeBuckets.add(fileSize);
                user.sumBlocks += fileBlocks;

                overallStats.sumFiles++;

//                map.put("accessTime", f.getAccessTime());
//                map.put("blockSize", f.getPreferredBlockSize());
//                map.put("group", p.getGroupName());
//                map.put("length", FSImageLoader.getFileSize(f));
//                map.put("modificationTime", f.getModificationTime());
//                map.put("owner", p.getUserName());
//                map.put("pathSuffix",inode.getName().toStringUtf8());
//                map.put("permission", FSImageLoader.toString(p.getPermission()));
//                map.put("replication", f.getReplication());
//                map.put("type", inode.getType());
//                map.put("fileId", inode.getId());
//                map.put("childrenNum", 0);
            }

            @Override
            public void onDirectory(FsImageProto.INodeSection.INode inode) {
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                PermissionStatus p = loader.getPermissionStatus(d.getPermission());

                // Group stats
                final String groupName = p.getGroupName();
                GroupStats groupStat;
                if (groupStats.containsKey(groupName)) {
                    groupStat = groupStats.get(groupName);
                } else {
                    groupStat = new GroupStats();
                    groupStat.groupName = groupName;
                    groupStats.put(groupName, groupStat);
                }
                groupStat.sumDirectories++;

                // User stats
                final String userName = p.getUserName();
                UserStats user;
                if (userStats.containsKey(userName)) {
                    user = userStats.get(userName);
                } else {
                    user = new UserStats();
                    user.userName = userName;
                    userStats.put(userName, user);
                }
                user.sumDirectories++;

                overallStats.sumDirectories++;
//                map.put("accessTime", 0);
//                map.put("blockSize", 0);
//                map.put("group", p.getGroupName());
//                map.put("length", 0);
//                map.put("modificationTime", d.getModificationTime());
//                map.put("owner", p.getUserName());
//                map.put("pathSuffix",inode.getName().toStringUtf8());
//                map.put("permission", FSImageLoader.toString(p.getPermission()));
//                map.put("replication", 0);
//                map.put("type", inode.getType());
//                map.put("fileId", inode.getId());
//                map.put("childrenNum", loader.getNumChildren(inode);
            }

            @Override
            public void onSymLink(FsImageProto.INodeSection.INode inode) {
                System.out.println("Symlink: " + inode.getName().toStringUtf8());
            }
        });
        log.info("Visiting finished.");

        System.out.println();
        System.out.println("HDFS Summary");
        System.out.println("------------");
        System.out.println();
        // Overall
        final String[] bucketUnits = FormatUtil.toStringSizeFormatted(overallStats.fileSizeBuckets.getBucketSizedInBytes());
        final int[] maxLength = FormatUtil.max(
                FormatUtil.length(bucketUnits),
                FormatUtil.numberOfDigits(overallStats.fileSizeBuckets.get()));
        final String bucketFormatValue = FormatUtil.formatForLengths(maxLength, "d");
        final String bucketFormatHeader = FormatUtil.formatForLengths(maxLength, "s");
        final String bucketHeader = String.format(bucketFormatHeader, (Object[]) bucketUnits);

        System.out.println(
                "#Groups  | #Users      | #Directories | #Files     | Size [MB] | #Blocks   | File Size Buckets ");
        String header2ndLine =
                "         |             |              |            |           |           | " + bucketHeader;
        System.out.println(header2ndLine);
        System.out.println(FormatUtil.padRight('-', header2ndLine.length()));

        System.out.println(String.format("%8d | %11d | %12d | %10d | %9d | %9d | %s",
                groupStats.size(), userStats.size(),
                overallStats.sumDirectories, overallStats.sumFiles, overallStats.sumFileSize / 1024L / 1024L,
                overallStats.sumBlocks,
                String.format(bucketFormatValue, Arrays.stream(overallStats.fileSizeBuckets.get()).boxed().toArray())
        ));
        System.out.println();

        // Groups
        System.out.println(String.format(
                "By group:     %8d | #Directories | #File      | Size [MB] | #Blocks   | File Size Buckets", groupStats.size()));
        header2ndLine = "     " +
                "                  |              |            |           |           | " + bucketHeader;
        System.out.println(header2ndLine);
        System.out.println(FormatUtil.padRight('-', header2ndLine.length()));
        for (GroupStats stat : sorted(groupStats.values(), options.sort)) {
            System.out.println(String.format("%22s |   %10d | %10d | %9d | %9d | %s",
                    stat.groupName, stat.sumDirectories, stat.sumFiles, stat.sumFileSize / 1024L / 1024L,
                    stat.sumBlocks,
                    String.format(bucketFormatValue, Arrays.stream(stat.fileSizeBuckets.get()).boxed().toArray())
            ));
        }

        // Users
        System.out.println();
        System.out.println(String.format(
                "By user:      %8d | #Directories | #File      | Size [MB] | #Blocks   | File Size Buckets", userStats.size()));
        header2ndLine = "     " +
                "                  |              |            |           |           | " + bucketHeader;
        System.out.println(header2ndLine);
        System.out.println(FormatUtil.padRight('-', header2ndLine.length()));
        for (UserStats stat : sorted(userStats.values(), options.sort)) {
            System.out.println(String.format("%22s |   %10d | %10d | %9d | %9d | %s",
                    stat.userName, stat.sumDirectories, stat.sumFiles, stat.sumFileSize / 1024L / 1024L,
                    stat.sumBlocks,
                    String.format(bucketFormatValue, Arrays.stream(stat.fileSizeBuckets.get()).boxed().toArray())
            ));
        }
    }

    private static <T extends AbstractStats> Collection<T> sorted(Collection<T> values, String sortOption) {
        if(null==sortOption||sortOption.isEmpty()) {
            return values;
        }

        final List<T> list = new ArrayList<T>(values);
        if (sortOption.equals("fs")) {
            list.sort(new Comparator<AbstractStats>() {
                @Override
                public int compare(AbstractStats o1, AbstractStats o2) {
                    return Long.valueOf(o1.sumFileSize).compareTo(o2.sumFileSize);
                }
            });
        } else if (sortOption.equals("fc")) {
            list.sort(new Comparator<AbstractStats>() {
                @Override
                public int compare(AbstractStats o1, AbstractStats o2) {
                    return Long.valueOf(o1.sumFiles).compareTo(o2.sumFiles);
                }
            });
        }
        return list;
    }

    public static void main(String[] args) throws IOException {
        Options options = new Options();
        CommandLine commandLine = new CommandLine(options);
        commandLine.parse(args);
        if (options.helpRequested) {
            commandLine.usage(System.out);
        } else {
            doPerform(options);
        }
    }

}
