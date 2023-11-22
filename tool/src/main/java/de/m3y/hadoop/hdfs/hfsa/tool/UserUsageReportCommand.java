package de.m3y.hadoop.hdfs.hfsa.tool;


import java.io.IOException;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.m3y.hadoop.hdfs.hfsa.core.*;
import de.m3y.hadoop.hdfs.hfsa.util.IECBinary;
import picocli.CommandLine;

/**
 * Computes top size locations for a user.
 */
@CommandLine.Command(name = "userusage", aliases = "uu",
        description = "Reports on top usage (e.g. size) locations of a user",
        mixinStandardHelpOptions = true,
        helpCommand = true,
        showDefaultValues = true
)
public class UserUsageReportCommand extends AbstractReportCommand {

    static class SizeReport {
        final Map<String, LongAdder> pathToSize = new ConcurrentHashMap<>();

        void increment(String path, long size) {
            pathToSize.computeIfAbsent(path, v -> new LongAdder()).add(size);
        }
    }

    @CommandLine.Option(names = {"-l", "--limit"},
            description = "Limits number of locations reported."
    )
    int hotspotsLimit = 20;

    static class AgeConverter implements CommandLine.ITypeConverter<Long> {
        static final Pattern PATTERN_AGE = Pattern.compile("(\\d+)([yYdDhHmM])?");

        @Override
        public Long convert(String ageOption) {
            if (null == ageOption || ageOption.isEmpty()) {
                return 0L;
            } else {
                return parseAge(ageOption);
            }
        }

        private long parseAge(String ageOption) {
            Matcher m = PATTERN_AGE.matcher(ageOption);
            if (!m.matches()) {
                throw new IllegalArgumentException("Expected value of pattern <"+PATTERN_AGE.pattern().replaceAll("[()]","")+">");
            }
            final int groupCount = m.groupCount();
            long factor = 0L;
            if (groupCount == 2) {
                String fg = m.group(2).toLowerCase();
                switch (fg) {
                    case "y":
                        factor = 365L * 24L * 60L * 60L * 1000L;
                        break;
                    case "d":
                        factor = 24L * 60L * 60L * 1000L;
                        break;
                    case "h":
                        factor = 60L * 60L * 1000L;
                        break;
                    default:
                        throw new IllegalStateException("Unsupported factor " + fg + ", option value is " + ageOption);
                }
            }
            long base = Long.parseLong(m.group(1));
            return base * factor;
        }
    }

    @CommandLine.Option(names = {"-a", "--age"},
            description = "Filters by age. For size, the file modification must be older than provided limit (eg 60d for older than 60 days)",
            converter = AgeConverter.class
    )
    long ageMs = 0L;

    @CommandLine.Parameters(paramLabel = "USER", arity = "1",
            description = "User name.")
    String user;

    @Override
    public void run() {
        if (null == user || user.isEmpty()) {
            throw new IllegalArgumentException("Expected USER as final argument");
        }
        final FsImageData fsImageData = loadFsImage();
        if (null != fsImageData) {
            for (String dir : mainCommand.dirs) {
                log.debug("Visiting {} ...", dir);
                long start = System.currentTimeMillis();
                final SizeReport report = computeReport(fsImageData, dir);
                log.info("Visiting directory {} finished [{}ms].", dir, System.currentTimeMillis() - start);

                handleReport(report, dir);

                if (mainCommand.dirs.length > 1) {
                    mainCommand.out.println();
                }
            }
        }
    }

    private void handleReport(SizeReport report, String dir) {
        PrintStream out = mainCommand.out;

        out.println();
        out.println("Size report " + printFilter(dir));
        out.println();

        if (report.pathToSize.isEmpty()) {
            out.println("No data found");
        } else {
            printUsersReport(out, report);
        }
    }

    private String printFilter(String dir) {
        if (ageMs > 0L) {
            String date = DateTimeFormatter.ISO_DATE_TIME.format(LocalDateTime.now().minus(ageMs, ChronoUnit.MILLIS));
            return "(user=" + user + ", start dir=" + dir + ", last modification older " + date + ")";
        } else
            return "(user=" + user + ", start dir=" + dir + ")";
    }

    private void printUsersReport(PrintStream out, SizeReport report) {
        int maxWidthPath = Math.max(1, report.pathToSize.keySet().stream()
                .mapToInt(String::length)
                .max()
                .orElse(0));
        final String format = "%-" + maxWidthPath + "s | %s%n";

        report.pathToSize.entrySet().stream()
                .sorted(REPORT_ENTRY_COMPARATOR)
                .limit(hotspotsLimit)
                .forEach(e ->
                        out.printf(format, e.getKey(), IECBinary.format(e.getValue().longValue())));
    }

    static final Comparator<Map.Entry<String, LongAdder>> REPORT_ENTRY_COMPARATOR = (o1, o2) -> {
        int c = Long.compare(o2.getValue().longValue(), o1.getValue().longValue()); // Inverted!
        if (0 == c) { // If same size, compare paths as secondary sort criteria
            return o1.getKey().compareTo(o2.getKey());
        }
        return c;
    };

    private SizeReport computeReport(FsImageData fsImageData, String dir) {
        SizeReport report = new SizeReport();

        long minAge = System.currentTimeMillis() - ageMs;
        try {
            FsVisitor visitor = new FsVisitor() {
                @Override
                public void onFile(FsImageFile fsImageFile) {
                    if (fsImageFile.getModificationTime() < minAge) {
                        if (user.equalsIgnoreCase(fsImageFile.getUser())) {
                            final long fileSizeBytes = fsImageFile.getFileSizeByte();
                            report.increment(fsImageFile.getPath(), fileSizeBytes);
                        }
                    }
                }

                @Override
                public void onDirectory(FsImageDir fsImageDir) {
                    // Not needed
                }

                @Override
                public void onSymLink(FsImageSymLink fsImageSymLink) {
                    // Not needed
                }
            };
            new FsVisitor.Builder().parallel().visit(fsImageData, visitor, dir);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        aggregatePaths(report.pathToSize);

        return report;
    }

    private void aggregatePaths(Map<String, LongAdder> pathToCounter) {
        final List<Map.Entry<String, LongAdder>> entries = new ArrayList<>(pathToCounter.entrySet());
        final Map<String, LongAdder> aggregates = new HashMap<>();
        for (Map.Entry<String, LongAdder> entry : entries) {
            String path = entry.getKey();
            if (FsImageData.ROOT_PATH.equals(path)) {
                continue;
            }
            long smallFilesCount = entry.getValue().longValue();
            for (int idx = path.lastIndexOf('/'); idx >= 0; idx = path.lastIndexOf('/', idx - 1)) {
                String parentPath = (0 == idx ? FsImageData.ROOT_PATH : path.substring(0, idx));
                aggregates.computeIfAbsent(parentPath, v -> new LongAdder()).add(smallFilesCount);
            }
        }
        for (Map.Entry<String, LongAdder> entry : aggregates.entrySet()) {
            pathToCounter.computeIfAbsent(entry.getKey(), v -> new LongAdder()).add(entry.getValue().longValue());
        }
    }
}
