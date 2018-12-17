package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import de.m3y.hadoop.hdfs.hfsa.util.SizeBucket;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.log4j.Level;
import org.apache.log4j.spi.RootLogger;
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
        long sumSymLinks;
        long sumBlocks;
        long sumFileSize;
        final SizeBucket fileSizeBuckets;

        AbstractStats() {
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
        final String dirPath;

        Report(String dirPath) {
            this.dirPath = dirPath;
            groupStats = new ConcurrentHashMap<>();
            userStats = new ConcurrentHashMap<>();
            overallStats = new OverallStats();
        }

        GroupStats getOrCreateGroupStats(String groupName) {
            return groupStats.computeIfAbsent(groupName, GroupStats::new);
        }

        UserStats getOrCreateUserStats(String userName) {
            return userStats.computeIfAbsent(userName, UserStats::new);
        }
    }

    static void doPerform(CliOptions options, PrintStream out) throws IOException {
        RandomAccessFile file = new RandomAccessFile(options.fsImageFile, "r");
        final FSImageLoader loader = FSImageLoader.load(file);

        // Check options
        if (null == options.dirs || options.dirs.length == 0) {
            options.dirs = new String[]{"/"}; // Default
        }

        for (String dir : options.dirs) {
            LOG.info("Visiting " + dir + " ...");
            long start = System.currentTimeMillis();
            final Report report = computeReport(loader, dir);
            LOG.info("Visiting finished [" + (System.currentTimeMillis() - start) + "ms].");

            doSummary(options, report, out);
        }
    }

    static void doSummary(CliOptions options, Report report, PrintStream out) {
        // Overall
        final OverallStats overallStats = report.overallStats;

        out.println();
        final String title = "HDFS Summary : " + report.dirPath;
        out.println(title);
        out.println(FormatUtil.padRight('-', title.length()));
        out.println();

        // Overall
        final String[] bucketUnits = FormatUtil.toStringSizeFormatted(
                overallStats.fileSizeBuckets.computeBucketUpperBorders());
        final int[] maxLength = FormatUtil.max(
                FormatUtil.length(bucketUnits),
                FormatUtil.numberOfDigits(overallStats.fileSizeBuckets.get()));
        final String bucketFormatValue = FormatUtil.formatForLengths(maxLength, "d");
        final String bucketFormatHeader = FormatUtil.formatForLengths(maxLength, "s");
        final String bucketHeader = String.format(bucketFormatHeader, (Object[]) bucketUnits);

        out.println(
                "#Groups  | #Users      | #Directories | #Symlinks |  #Files     | Size [MB] | #Blocks   | File Size Buckets ");
        String header2ndLine =
                "         |             |              |           |             |           |           | " + bucketHeader;
        out.println(header2ndLine);
        out.println(FormatUtil.padRight('-', header2ndLine.length()));

        out.println(String.format("%8d | %11d | %12d | %9d | %10d | %9d | %9d | %s",
                report.groupStats.size(), report.userStats.size(),
                overallStats.sumDirectories, overallStats.sumSymLinks,
                overallStats.sumFiles, overallStats.sumFileSize / 1024L / 1024L,
                overallStats.sumBlocks,
                String.format(bucketFormatValue,
                        FormatUtil.boxAndPadWithZeros(maxLength.length, overallStats.fileSizeBuckets.get()))
        ));
        out.println();

        // Groups
        out.println(String.format(
                "By group:     %8d | #Directories | #SymLinks | #File      | Size [MB] | #Blocks   | File Size Buckets",
                report.groupStats.size()));
        header2ndLine = "     " +
                "                  |              |           |            |           |           | " + bucketHeader;
        out.println(header2ndLine);
        out.println(FormatUtil.padRight('-', header2ndLine.length()));
        for (GroupStats stat : sorted(report.groupStats.values(), options.sort)) {
            out.println(String.format("%22s |   %10d | %9d | %10d | %9d | %9d | %s",
                    stat.groupName, stat.sumDirectories, stat.sumSymLinks,
                    stat.sumFiles, stat.sumFileSize / 1024L / 1024L,
                    stat.sumBlocks,
                    String.format(bucketFormatValue,
                            FormatUtil.boxAndPadWithZeros(maxLength.length, stat.fileSizeBuckets.get()))
            ));
        }

        // Users
        out.println();
        final List<UserStats> userStats = filter(report.userStats.values(), options);
        out.println(String.format(
                "By user:      %8d | #Directories | #SymLinks | #File      | Size [MB] | #Blocks   | File Size Buckets",
                userStats.size()));
        header2ndLine = "     " +
                "                  |              |           |            |           |           | " + bucketHeader;
        out.println(header2ndLine);
        out.println(FormatUtil.padRight('-', header2ndLine.length()));
        for (UserStats stat : sorted(userStats, options.sort)) {
            out.println(String.format("%22s |   %10d | %9d | %10d | %9d | %9d | %s",
                    stat.userName, stat.sumDirectories, stat.sumSymLinks,
                    stat.sumFiles, stat.sumFileSize / 1024L / 1024L,
                    stat.sumBlocks,
                    String.format(bucketFormatValue,
                            FormatUtil.boxAndPadWithZeros(maxLength.length, stat.fileSizeBuckets.get()))
            ));
        }
    }

    static List<UserStats> filter(Collection<UserStats> userStats, CliOptions options) {
        List<UserStats> filtered = new ArrayList<>(userStats);
        // user name
        if (null != options.userNameFilter && !options.userNameFilter.isEmpty()) {
            Pattern userNamePattern = Pattern.compile(options.userNameFilter);
            filtered.removeIf(u -> !userNamePattern.matcher(u.userName).find());
        }
        return filtered;
    }

    static Report computeReport(FSImageLoader loader, String dirPath) throws IOException {
        final Report report = new Report(dirPath);
        final OverallStats overallStats = report.overallStats;

        loader.visitParallel(new FsVisitor() {
            @Override
            public void onFile(FsImageProto.INodeSection.INode inode, String path) {
                FsImageProto.INodeSection.INodeFile f = inode.getFile();

                PermissionStatus p = loader.getPermissionStatus(f.getPermission());

                final long fileSize = FSImageLoader.getFileSize(f);
                final long fileBlocks = f.getBlocksCount();
                synchronized (overallStats) {
                    overallStats.fileSizeBuckets.add(fileSize);
                    overallStats.sumBlocks += fileBlocks;
                    overallStats.sumFileSize += fileSize;
                    overallStats.sumFiles++;
                }

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.getOrCreateGroupStats(groupName);
                synchronized (groupStat) {
                    groupStat.sumFiles++;
                    groupStat.sumFileSize += fileSize;
                    groupStat.fileSizeBuckets.add(fileSize);
                    groupStat.sumBlocks += fileBlocks;
                }

                // User stats
                final String userName = p.getUserName();
                final UserStats userStat = report.getOrCreateUserStats(userName);
                synchronized (userStat) {
                    userStat.sumFiles++;
                    userStat.sumFileSize += fileSize;
                    userStat.fileSizeBuckets.add(fileSize);
                    userStat.sumBlocks += fileBlocks;
                }
            }

            @Override
            public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                PermissionStatus p = loader.getPermissionStatus(d.getPermission());

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.getOrCreateGroupStats(groupName);
                synchronized (groupStat) {
                    groupStat.sumDirectories++;
                }

                // User stats
                final String userName = p.getUserName();
                final UserStats userStat = report.getOrCreateUserStats(userName);
                synchronized (userStat) {
                    userStat.sumDirectories++;
                }

                synchronized (overallStats) {
                    overallStats.sumDirectories++;
                }
            }

            @Override
            public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
                System.out.println("Ignoring symlink: " + inode.getName().toStringUtf8());
                final FsImageProto.INodeSection.INodeSymlink symlink = inode.getSymlink();
                if (symlink.hasPermission()) {
                    PermissionStatus p = loader.getPermissionStatus(symlink.getPermission());

                    // Group stats
                    final String groupName = p.getGroupName();
                    final GroupStats groupStat = report.getOrCreateGroupStats(groupName);
                    synchronized (groupStat) {
                        groupStat.sumSymLinks++;
                    }

                    // User stats
                    final String userName = p.getUserName();
                    final UserStats userStat = report.getOrCreateUserStats(userName);
                    synchronized (userStat) {
                        userStat.sumSymLinks++;
                    }

                    synchronized (overallStats) {
                        overallStats.sumSymLinks++;
                    }
                }
                overallStats.sumSymLinks++;

            }
        }, dirPath);

        return report;
    }

    private static <T extends AbstractStats> Collection<T> sorted(Collection<T> values, String sortOption) {
        switch (sortOption) {
            case "bc":
                return sortStats(values, Comparator.comparingLong(o -> o.sumBlocks));
            case "fc":
                return sortStats(values, Comparator.comparingLong(o3 -> o3.sumFiles));
            case "dc":
                return sortStats(values, Comparator.comparingLong(o2 -> o2.sumDirectories));
            case "fs": // default sort
                return sortStats(values, Comparator.comparingLong(o -> o.sumFileSize));
            default:
                throw new IllegalArgumentException("Unsupported sort option " + sortOption);
        }
    }

    private static <T extends AbstractStats> List<T> sortStats(Collection<T> values, Comparator<T> comparator) {
        final List<T> list = new ArrayList<>(values);
        list.sort(comparator);
        return list;
    }

    public static void main(String[] args) throws IOException {
        PrintStream out = System.out;
        PrintStream err = System.err;
        CliOptions options = new CliOptions();
        CommandLine commandLine = new CommandLine(options);
        try {
            commandLine.parse(args);
            if (null != options.verbose) {
                if(options.verbose.length==1) {
                    RootLogger.getRootLogger().setLevel(Level.INFO);
                } else {
                    RootLogger.getRootLogger().setLevel(Level.DEBUG);
                }
            }
            if (options.helpRequested) {
                commandLine.usage(out);
            } else {
                doPerform(options, out);
            }
        } catch (CommandLine.ParameterException ex) {
            err.println("Invalid options : " + ex.getMessage());
            err.println();
            commandLine.usage(err);
        }
    }

}
