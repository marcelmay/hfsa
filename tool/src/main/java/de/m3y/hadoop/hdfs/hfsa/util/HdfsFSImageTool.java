package de.m3y.hadoop.hdfs.hfsa.util;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * HDFS FSImage Tool extracts a summary of HDFS Usage from fsimage.
 */
public class HdfsFSImageTool {
    private static final Logger log = getLogger(HdfsFSImageTool.class);


    static class UserStats {
        String userName;
        long sumFiles;
        long sumBlocks;
        long sumFileSize;
        long sumDirectories;
        SizeBucket fileSizeBuckets = new SizeBucket();

        @Override
        public String toString() {
            return "UserStats{" +
                    "userName='" + userName + '\'' +
                    ", numFiles=" + sumFiles +
                    ", sumFileSize=" + sumFileSize +
                    ", numDirectories=" + sumDirectories +
                    ", fileSizeBuckets=" + fileSizeBuckets +
                    '}';
        }
    }

    static class GroupStats {
        String groupName;
        long sumFiles;
        long sumBlocks;
        long sumFileSize;
        long sumDirectories;
        SizeBucket fileSizeBuckets = new SizeBucket();

        @Override
        public String toString() {
            return "GroupStats{" +
                    "groupName='" + groupName + '\'' +
                    ", numFiles=" + sumFiles +
                    ", sumFileSize=" + sumFileSize +
                    ", numDirectories=" + sumDirectories +
                    ", fileSizeBuckets=" + fileSizeBuckets +
                    '}';
        }
    }

    static class OverallStats {
        long sumFiles;
        long sumDirectories;
        long sumBlocks;
        long sumFileSize;
        final SizeBucket sizeBucket = new SizeBucket();
    }


    public static void main(String[] args) throws IOException {
        RandomAccessFile file = new RandomAccessFile(args[0], "r");
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
                overallStats.sizeBucket.add(fileSize);
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
        final String[] bucketUnits = FormatUtil.toStringSizeFormatted(overallStats.sizeBucket.getBucketSizedInBytes());
        final int[] maxLength = FormatUtil.max(
                FormatUtil.length(bucketUnits),
                FormatUtil.numberOfDigits(overallStats.sizeBucket.get()));
        final String bucketFormatValue = FormatUtil.formatForLengths(maxLength, "d");
        final String bucketFormatHeader = FormatUtil.formatForLengths(maxLength, "s");
        final String bucketHeader = String.format(bucketFormatHeader, (Object[])bucketUnits);

        System.out.println(
                "#Files     | #Blocks  | Size [MB] | #Dirs     | #Groups | #Users | File Size Buckets ");
        String header2ndLine = "           |          |           |           |         |        | " + bucketHeader;
        System.out.println( header2ndLine );
        System.out.println(FormatUtil.padRight('-', header2ndLine.length()));

        System.out.println(String.format("%10d | %8d | %9d | %9d | %7d | %6d | %s",
                overallStats.sumFiles, overallStats.sumBlocks, overallStats.sumFileSize / 1024L / 1024L,
                overallStats.sumDirectories, groupStats.size(), userStats.size(),
                String.format(bucketFormatValue, Arrays.stream(overallStats.sizeBucket.get()).boxed().toArray())
        ));
        System.out.println();

        // Groups
        System.out.println(String.format(
                "By group:     %8d | #Directories | #File   | Size [MB] | File Size Buckets", groupStats.size()));
        header2ndLine = "                       |              |         |           | " + bucketHeader;
        System.out.println(header2ndLine);
        System.out.println(FormatUtil.padRight('-', header2ndLine.length()));
        for (GroupStats stat : groupStats.values()) {
            System.out.println(String.format("%22s |   %10d | %7d | %9d | %s",
                    stat.groupName, stat.sumDirectories, stat.sumFiles, stat.sumFileSize / 1024L / 1024L,
                    String.format(bucketFormatValue, Arrays.stream(stat.fileSizeBuckets.get()).boxed().toArray())
            ));
        }

        // Users
        System.out.println();
        System.out.println(String.format(
                "By user:      %8d | #Directories | #File   | Size [MB] | File Size Buckets", userStats.size()));
        header2ndLine = "                       |              |         |           | " + bucketHeader;
        System.out.println(header2ndLine);
        System.out.println(FormatUtil.padRight('-', header2ndLine.length()));
        for (UserStats stat : userStats.values()) {
            System.out.println(String.format("%22s |   %10d | %7d | %9d | %s",
                    stat.userName, stat.sumDirectories, stat.sumFiles, stat.sumFileSize / 1024L / 1024L,
                    String.format(bucketFormatValue, Arrays.stream(stat.fileSizeBuckets.get()).boxed().toArray())
            ));
        }
    }

}
