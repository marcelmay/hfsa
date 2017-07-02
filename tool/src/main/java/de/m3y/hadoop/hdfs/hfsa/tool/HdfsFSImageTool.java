package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.regex.Pattern;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import de.m3y.hadoop.hdfs.hfsa.util.SizeBucket;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;
import picocli.CommandLine;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * HDFS FSImage Tool extracts a summary of HDFS Usage from fsimage.
 */
public class HdfsFSImageTool {
    private static final Logger LOG = getLogger(HdfsFSImageTool.class);

    abstract static class AbstractStats {
        long sumFiles;
        long sumDirectories;
        long sumBlocks;
        long sumFileSize;
        final SizeBucket fileSizeBuckets;

        protected AbstractStats() {
            fileSizeBuckets = new SizeBucket();
        }
    }

    static class UserStats extends AbstractStats {
        final String userName;

        UserStats(String userName) {
            this.userName = userName;
        }
    }

    static class GroupStats extends AbstractStats {
        final String groupName;

        GroupStats(String groupName) {
            this.groupName = groupName;
        }
    }

    static class OverallStats extends AbstractStats {
    }

    static class Report {
        final Map<String, GroupStats> groupStats;
        final Map<String, UserStats> userStats;
        final OverallStats overallStats;

        Report() {
            groupStats = new HashMap<>();
            userStats = new HashMap<>();
            overallStats = new OverallStats();
        }

        GroupStats getOrCreateGroupStats(String groupName) {
            GroupStats stat = groupStats.get(groupName);
            if (null == stat) {
                stat = new GroupStats(groupName);
                groupStats.put(groupName, stat);
            }
            return stat;
        }

        UserStats getOrCreateUserStats(String userName) {
            UserStats stat = userStats.get(userName);
            if (null == stat) {
                stat = new UserStats(userName);
                userStats.put(userName, stat);
            }
            return stat;
        }
    }

    static void doPerform(CliOptions options) throws IOException {
        RandomAccessFile file = new RandomAccessFile(options.fsImageFile, "r");
        final FSImageLoader loader = FSImageLoader.load(file);

        // Check options
        if (null == options.dirs || options.dirs.length == 0) {
            options.dirs = new String[]{"/"}; // Default
        }

        for (String dir : options.dirs) {
            doSummary(options, loader, dir);
        }
    }

    private static void doSummary(CliOptions options, FSImageLoader loader, String dir) throws IOException {
        LOG.info("Visting " + dir + " ...");
        long start = System.currentTimeMillis();
        final Report report = computeReport(loader, dir);
        LOG.info("Visiting finished [" + (System.currentTimeMillis() - start) + "].");

        // Overall
        final OverallStats overallStats = report.overallStats;

        System.out.println();
        final String title = "HDFS Summary : " + dir;
        System.out.println(title);
        System.out.println(FormatUtil.padRight('-', title.length()));
        System.out.println();

        // Overall
        final String[] bucketUnits = FormatUtil.toStringSizeFormatted(overallStats.fileSizeBuckets.computeBucketUpperBorders());
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
                report.groupStats.size(), report.userStats.size(),
                overallStats.sumDirectories, overallStats.sumFiles, overallStats.sumFileSize / 1024L / 1024L,
                overallStats.sumBlocks,
                String.format(bucketFormatValue, Arrays.stream(overallStats.fileSizeBuckets.get()).boxed().toArray())
        ));
        System.out.println();

        // Groups
        System.out.println(String.format(
                "By group:     %8d | #Directories | #File      | Size [MB] | #Blocks   | File Size Buckets",
                report.groupStats.size()));
        header2ndLine = "     " +
                "                  |              |            |           |           | " + bucketHeader;
        System.out.println(header2ndLine);
        System.out.println(FormatUtil.padRight('-', header2ndLine.length()));
        for (GroupStats stat : sorted(report.groupStats.values(), options.sort)) {
            System.out.println(String.format("%22s |   %10d | %10d | %9d | %9d | %s",
                    stat.groupName, stat.sumDirectories, stat.sumFiles, stat.sumFileSize / 1024L / 1024L,
                    stat.sumBlocks,
                    String.format(bucketFormatValue, Arrays.stream(stat.fileSizeBuckets.get()).boxed().toArray())
            ));
        }

        // Users
        System.out.println();
        final List<UserStats> userStats = filter(report.userStats.values(), options);
        System.out.println(String.format(
                "By user:      %8d | #Directories | #File      | Size [MB] | #Blocks   | File Size Buckets",
                userStats.size()));
        header2ndLine = "     " +
                "                  |              |            |           |           | " + bucketHeader;
        System.out.println(header2ndLine);
        System.out.println(FormatUtil.padRight('-', header2ndLine.length()));
        for (UserStats stat : sorted(userStats, options.sort)) {
            System.out.println(String.format("%22s |   %10d | %10d | %9d | %9d | %s",
                    stat.userName, stat.sumDirectories, stat.sumFiles, stat.sumFileSize / 1024L / 1024L,
                    stat.sumBlocks,
                    String.format(bucketFormatValue, Arrays.stream(stat.fileSizeBuckets.get()).boxed().toArray())
            ));
        }
    }

    static List<UserStats> filter(Collection<UserStats> userStats, CliOptions options) {
        List<UserStats> filtered = new ArrayList<>(userStats);
        // user name
        if(null != options.userFilter && !options.userFilter.isEmpty()) {
            Pattern userNamePattern = Pattern.compile(options.userFilter);
            filtered.removeIf(u -> !userNamePattern.matcher(u.userName).find());
        }
        return filtered;
    }

    private static Report computeReport(FSImageLoader loader, String dir) throws IOException {
        final Report report = new Report();
        final OverallStats overallStats = report.overallStats;

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
                GroupStats groupStat = report.getOrCreateGroupStats(groupName);
                groupStat.sumFiles++;
                groupStat.sumFileSize += fileSize;
                groupStat.fileSizeBuckets.add(fileSize);
                groupStat.sumBlocks += fileBlocks;

                // User stats
                final String userName = p.getUserName();
                UserStats user = report.getOrCreateUserStats(userName);
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
                GroupStats groupStat = report.getOrCreateGroupStats(groupName);
                groupStat.sumDirectories++;

                // User stats
                final String userName = p.getUserName();
                UserStats user = report.getOrCreateUserStats(userName);
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
        }, dir);

        return report;
    }

    private static <T extends AbstractStats> Collection<T> sorted(Collection<T> values, String sortOption) {
        switch (sortOption) {
            case "bc":
                return sortStats(values, new Comparator<T>() {
                    @Override
                    public int compare(AbstractStats o1, AbstractStats o2) {
                        return Long.valueOf(o1.sumBlocks).compareTo(o2.sumBlocks);
                    }
                });
            case "fc":
                return sortStats(values, new Comparator<T>() {
                    @Override
                    public int compare(AbstractStats o1, AbstractStats o2) {
                        return Long.valueOf(o1.sumFiles).compareTo(o2.sumFiles);
                    }
                });
            case "dc":
                return sortStats(values, new Comparator<T>() {
                    @Override
                    public int compare(AbstractStats o1, AbstractStats o2) {
                        return Long.valueOf(o1.sumDirectories).compareTo(o2.sumDirectories);
                    }
                });
            case "fs": // default sort
                return sortStats(values, new Comparator<T>() {
                    @Override
                    public int compare(AbstractStats o1, AbstractStats o2) {
                        return Long.valueOf(o1.sumFileSize).compareTo(o2.sumFileSize);
                    }
                });
            default:
                throw new IllegalArgumentException("Unsupported sort option "+sortOption);
        }
    }

    private static <T extends AbstractStats> List<T> sortStats(Collection<T> values, Comparator<T> comparator) {
        final List<T> list = new ArrayList<T>(values);
        list.sort(comparator);
        return list;
    }

    public static void main(String[] args) throws IOException {
        CliOptions options = new CliOptions();
        CommandLine commandLine = new CommandLine(options);
        try {
            commandLine.parse(args);
            if (options.helpRequested) {
                commandLine.usage(System.out);
            } else {
                doPerform(options);
            }
        } catch (CommandLine.ParameterException ex) {
            System.err.println("Invalid options : " + ex.getMessage());
            System.out.println();
            commandLine.usage(System.err);
        }
    }

}
