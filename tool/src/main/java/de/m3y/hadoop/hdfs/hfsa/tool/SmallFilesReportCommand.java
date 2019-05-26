package de.m3y.hadoop.hdfs.hfsa.tool;


import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
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

        static final Comparator<UserReport> COMPARATOR_USER_REPORT = Comparator.comparingLong(o -> o.sumSmallFiles);

        List<UserReport> listUserReports() {
            return userToReport.values().stream()
                    .sorted(COMPARATOR_USER_REPORT.reversed())
                    .collect(Collectors.toList());
        }
    }

    static class IECBinaryConverter implements CommandLine.ITypeConverter<Long> {
        @Override
        public Long convert(String value) throws Exception {
            try {
                return IECBinary.parse(value);
            } catch (Exception ex) {
                throw new CommandLine.TypeConversionException("'" + value + "' is not an IEC binary formatted value");
            }
        }
    }

    @CommandLine.Option(names = {"-fileSizeLimit", "--fsl"},
            description = "Small file size limit in bytes (IEC binary formatted, eg 2MiB). " +
                    "Every file less equals the limit counts as a small file.",
            converter = IECBinaryConverter.class
    )
    long fileSizeLimitBytes = 1024L * 1024L * 2L; // 2 MiB as default

    @CommandLine.Option(names = {"-userPathHotspotLimit", "--uphl"},
            description = "Limit of directory hotspots containing most small files."
    )
    int hotspotsLimit = 10;

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

        printOverallReport(report, out);

        final List<UserReport> userReports = report.listUserReports();
        if (!userReports.isEmpty()) {
            printUsersReport(out, userReports, report.sumOverallSmallFiles);
        } else {
            out.println("No users found in directory paths " + Arrays.toString(mainCommand.dirs));
        }
    }

    private void printOverallReport(Report report, PrintStream out) {
        final String formatSpec = FormatUtil.numberOfDigitsFormat(report.sumOverallSmallFiles) + "%n";
        if (report.sumOverallSmallFiles != report.sumUserSmallFiles) {
            out.printf("Overall small files         : " + formatSpec, report.sumOverallSmallFiles);
            out.printf("User (filtered) small files : " + formatSpec, +report.sumUserSmallFiles);
        } else {
            out.printf("Overall small files : " + formatSpec, report.sumOverallSmallFiles);
        }
        out.println();

        aggregatePaths(report.pathToCounter);
        final Comparator<Map.Entry<String, LongAdder>> comparator =
                Comparator.comparingLong(o -> o.getValue().longValue());
        final List<Map.Entry<String, LongAdder>> topEntries = report.pathToCounter.entrySet().stream()
                .sorted(comparator.reversed())
                .limit(hotspotsLimit)
                .collect(Collectors.toList());
        String labelCount = "#Small files ";
        int maxWidthSum = Math.max(FormatUtil.numberOfDigits(report.sumOverallSmallFiles), labelCount.length());
        String header = labelCount + " | Path (top " + this.hotspotsLimit + ") ";
        out.println(header);
        out.println(FormatUtil.padRight('-', header.length()));
        String format = "%" + maxWidthSum + "d | %s%n";
        if (!topEntries.isEmpty()) {
            out.printf(format, topEntries.get(0).getValue().longValue(), topEntries.get(0).getKey());
            for (int i = 1; i < topEntries.size(); i++) {
                final Map.Entry<String, LongAdder> entry = topEntries.get(i);
                out.printf(format, entry.getValue().longValue(), entry.getKey());
            }
        }
        out.println();

        // Free memory after reporting
        report.pathToCounter.clear();
    }

    private void printUsersReport(PrintStream out, List<UserReport> userReports, long sumOverallSmallFiles) {
        int maxWidthSum = Math.max(FormatUtil.numberOfDigits(userReports.get(0).sumSmallFiles), "#Small files".length());
        int maxWidthUserName = Math.max(
                userReports.stream()
                        .max(Comparator.comparingLong(o -> o.userName.length()))
                        .orElseThrow(IllegalStateException::new).userName.length(),
                "Username".length()
        );
        out.printf("%-" + maxWidthUserName + "." + maxWidthUserName + "s | %-" + maxWidthSum + "." + maxWidthSum + "s | %s%n",
                "Username", "#Small files","%");
        out.println(FormatUtil.padRight('-', maxWidthUserName + 3 + maxWidthSum + 3 + 10));
        final String format = "%-" + maxWidthUserName + "s | %" + maxWidthSum + "d | %3.1f%%%n";
        for (UserReport userReport : userReports) {
            final float percentage = (float)userReport.sumSmallFiles / sumOverallSmallFiles * 100f;
            out.printf(format, userReport.userName, userReport.sumSmallFiles, percentage);
        }

        out.println();

        // Details including top directory small files hotspots.
        // Free memory of user reports not in hotspot list.
        userReports.subList(Math.min(hotspotsLimit, userReports.size()), userReports.size()).clear();

        final String hotspotLabel = "Small files hotspots (top " + hotspotsLimit + " count/path)";
        out.printf("%-" + maxWidthUserName + "." + maxWidthUserName + "s | %s%n",
                "Username", hotspotLabel);
        int separatorLength = maxWidthUserName + 3 + hotspotLabel.length();
        out.println(FormatUtil.padRight('-', separatorLength));
        for (int i = 0; i < Math.min(10, userReports.size()); i++) {
            final UserReport userReport = userReports.get(i);
            printUserDetailsReport(out, userReport, maxWidthUserName, maxWidthSum, separatorLength);
            // Free memory after reporting
            userReport.pathToCounter.clear();
        }
    }

    static final Comparator<Map.Entry<String, LongAdder>> USER_REPORT_ENTRY_COMPARATOR = (o1, o2) -> {
        int c = Long.compare(o1.getValue().longValue(), o2.getValue().longValue());
        if (0 == c) { // If same size, compare paths as secondary sort criteria
            return o1.getKey().compareTo(o2.getKey());
        }
        return -c;
    };

    private void printUserDetailsReport(PrintStream out, UserReport userReport, int maxWidthUserName, int maxWidthSum,
                                        int separatorLength) {
        aggregatePaths(userReport.pathToCounter);

        final List<Map.Entry<String, LongAdder>> topEntries = userReport.pathToCounter.entrySet().stream()
                .sorted(USER_REPORT_ENTRY_COMPARATOR)
                .limit(hotspotsLimit)
                .collect(Collectors.toList());
        String format = "%-" + maxWidthUserName + "." + maxWidthUserName + "s | %" + maxWidthSum + "d | %s%n";
        if (!topEntries.isEmpty()) {
            out.printf(format, userReport.userName, topEntries.get(0).getValue().longValue(), topEntries.get(0).getKey());
            for (int i = 1; i < topEntries.size(); i++) {
                final Map.Entry<String, LongAdder> entry = topEntries.get(i);
                out.printf(format, "", entry.getValue().longValue(), entry.getKey());
            }
        }
        out.println(FormatUtil.padRight('-', separatorLength));
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

    private void aggregatePaths(Map<String, LongAdder> pathToCounter) {
        final ArrayList<Map.Entry<String, LongAdder>> entries = new ArrayList<>(pathToCounter.entrySet());
        final Map<String, LongAdder> aggregates = new HashMap<>();
        for (Map.Entry<String, LongAdder> entry : entries) {
            String path = entry.getKey();
            if (FSImageLoader.ROOT_PATH.equals(path)) {
                continue;
            }
            long smallFilesCount = entry.getValue().longValue();
            for (int idx = path.lastIndexOf('/'); idx >= 0; idx = path.lastIndexOf('/', idx - 1)) {
                String parentPath = (0 == idx ? FSImageLoader.ROOT_PATH : path.substring(0, idx));
                aggregates.computeIfAbsent(parentPath, v -> new LongAdder()).add(smallFilesCount);
            }
        }
        for (Map.Entry<String, LongAdder> entry : aggregates.entrySet()) {
            pathToCounter.computeIfAbsent(entry.getKey(), v -> new LongAdder()).add(entry.getValue().longValue());
        }
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
