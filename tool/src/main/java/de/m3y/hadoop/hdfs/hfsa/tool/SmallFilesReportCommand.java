package de.m3y.hadoop.hdfs.hfsa.tool;


import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import de.m3y.hadoop.hdfs.hfsa.util.IECBinary;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import picocli.CommandLine;

/**
 * Computes a user/group small file summary
 */
@CommandLine.Command(name = "smallfiles", aliases = "sf",
        description = "Reports on small file usage",
        mixinStandardHelpOptions = true,
        helpCommand = true,
        showDefaultValues = true
)
public class SmallFilesReportCommand extends AbstractReportCommand {

    static class UserReport {
        final String userName;
        final Map<String, LongAdder> pathToCounter = new ConcurrentHashMap<>();
        long sumSmallFiles = 0;

        UserReport(String userName) {
            this.userName = userName;
        }

        void increment(String path) {
            pathToCounter.computeIfAbsent(path, v -> new LongAdder()).increment();
        }

        void computeStats() {
            sumSmallFiles = pathToCounter.values().stream().mapToLong(LongAdder::longValue).sum();
        }
    }

    static class Report {
        final Map<String, UserReport> userToReport = new ConcurrentHashMap<>();
        // Overall directory path to small file count mapping
        final Map<String, LongAdder> pathToCounter = new ConcurrentHashMap<>();
        long sumUserSmallFiles;
        long sumOverallSmallFiles;

        UserReport getOrCreateUserReport(String userName) {
            return userToReport.computeIfAbsent(userName, UserReport::new);
        }

        void increment(String path) {
            pathToCounter.computeIfAbsent(path, v -> new LongAdder()).increment();
        }

        void computeStats() {
            for (UserReport userReport : userToReport.values()) {
                userReport.computeStats();
                sumUserSmallFiles += userReport.sumSmallFiles;
            }

            sumOverallSmallFiles = pathToCounter.values().stream().mapToLong(LongAdder::longValue).sum();

            // TODO: Filter user report by min small files limit?
        }

        List<UserReport> listUserReports() {
            final Comparator<UserReport> comparator = Comparator.comparingLong(o -> o.sumSmallFiles);
            return userToReport.values().stream()
                    .sorted(comparator.reversed())
                    .collect(Collectors.toList());
        }
    }

    static class IECBinaryConverter implements CommandLine.ITypeConverter<Long> {
        @Override
        public Long convert(String value) throws Exception {
            try {
                return IECBinary.parse(value);
            } catch (Exception ex) {
                throw new CommandLine.TypeConversionException("'" + value + "' is not a IEC binary formatted value");
            }
        }
    }

    @CommandLine.Option(names = {"-fileSizeLimit", "--fsl"},
            description = "Small file size limit in bytes (IEC binary formatted, eg 2MiB). " +
                    "Every file less equals the limit counts as a small file.",
            converter = IECBinaryConverter.class
    )
    long fileSizeLimitBytes = 1024L * 1024L * 2L; // 2 MiB

    @Override
    public void run() {
        final FSImageLoader loader = loadFsImage();
        if (null != loader) {
            for (String dir : mainCommand.dirs) {
                log.info("Visiting {} ...", dir);
                long start = System.currentTimeMillis();
                final Report report = computeReport(loader, dir);
                log.info("Visiting finished [{}ms].", System.currentTimeMillis() - start);

                handleReport(report);
            }
        }
    }

    private void handleReport(Report report) {
        PrintStream out = mainCommand.out;

        out.println();
        out.println("Small files report (< " + IECBinary.format(fileSizeLimitBytes) + ")");
        out.println();

        final String formatSpec = FormatUtil.numberOfDigitsFormat(report.sumOverallSmallFiles) + "%n";
        if(report.sumOverallSmallFiles != report.sumUserSmallFiles) {
            out.printf("Overall small files         : " + formatSpec, report.sumOverallSmallFiles);
            out.printf("User (filtered) small files : " + formatSpec, +report.sumUserSmallFiles);
        } else {
            out.printf("Overall small files : " + formatSpec, report.sumOverallSmallFiles);
        }
        out.println();

        final List<UserReport> userReports = report.listUserReports();
        if (!userReports.isEmpty()) {
            int maxWidthSum = Math.max(FormatUtil.numberOfDigits(userReports.get(0).sumSmallFiles), "Sum small files".length());
            int maxWidthUserName = Math.max(FormatUtil.numberOfDigits(
                    userReports.stream()
                            .max(Comparator.comparingLong(o -> o.userName.length()))
                            .orElseThrow(IllegalStateException::new).userName.length()),
                    "Username".length()
            );
            out.printf("%-" + maxWidthUserName + "." + maxWidthUserName + "s | %-" + maxWidthSum + "." + maxWidthSum + "s%n",
                    "Username", "Sum small files");
            out.println(FormatUtil.padRight('-', maxWidthUserName + 3 + maxWidthSum));
            final String format = "%-" + maxWidthUserName + "s | %" + maxWidthSum + "d%n";
            for (UserReport userReport : userReports) {
                out.printf(format, userReport.userName, userReport.sumSmallFiles);
            }
        } else {
            out.println("No users found in directory paths " + Arrays.toString(mainCommand.dirs));
        }
    }


    private Report computeReport(FSImageLoader loader, String dir) {
        Report report = new Report();
        Predicate<String> userNameFilter = createUserNameFilter(mainCommand.userNameFilter);

        try {
            FsVisitor visitor = new FsVisitor() {
                @Override
                public void onFile(FsImageProto.INodeSection.INode inode, String path) {
                    FsImageProto.INodeSection.INodeFile f = inode.getFile();
                    final long fileSizeBytes = FSImageLoader.getFileSize(f);
                    if (fileSizeBytes < fileSizeLimitBytes) {
                        PermissionStatus p = loader.getPermissionStatus(f.getPermission());
                        if (userNameFilter.test(p.getUserName())) {
                            report.getOrCreateUserReport(p.getUserName()).increment(path);
                        }
                        report.increment(path);
                    }
                }

                @Override
                public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
                    // Not needed
                }

                @Override
                public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
                    // Not needed
                }
            };
            loader.visitParallel(visitor, dir);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        report.computeStats();

        return report;
    }

    private Predicate<String> createUserNameFilter(String userNameFilter) {
        if (null != userNameFilter && !userNameFilter.isEmpty()) {
            Pattern userNamePattern = Pattern.compile(mainCommand.userNameFilter);
            ThreadLocal<Matcher> localMatcher = ThreadLocal.withInitial(() -> userNamePattern.matcher(""));
            return value -> localMatcher.get().reset(value).matches();
        } else {
            return value -> true;
        }
    }
}
