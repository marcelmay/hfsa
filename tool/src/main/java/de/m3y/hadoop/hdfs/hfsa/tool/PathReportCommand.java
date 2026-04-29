package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.jspecify.annotations.NonNull;
import picocli.CommandLine;

/**
 * Reports details about an inode structure.
 */
@CommandLine.Command(name = "path", aliases = "p",
        description = "Lists INode paths",
        mixinStandardHelpOptions = true,
        helpCommand = true,
        showDefaultValues = true
)
public class PathReportCommand extends AbstractReportCommand {

    /**
     * @param permission user, group, FS permissions
     */
    record Result(long permission, String path, char iNodeType) {
    }

    private static class ResultTypeAdapter extends TypeAdapter<Result> {
        private final FsImageData fsImageData;

        public ResultTypeAdapter(FsImageData fsImageData) {
            this.fsImageData = fsImageData;
        }

        @Override
        public void write(JsonWriter out, Result value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                final PermissionStatus permissionStatus = fsImageData.getPermissionStatus(value.permission);
                out.beginObject()
                        .name("path").value(value.path)
                        .name("user").value(permissionStatus.getUserName())
                        .name("group").value(permissionStatus.getGroupName())
                        .name("type").value(switch (value.iNodeType) {
                            case '-' -> "f";
                            case 'd' -> "d";
                            case 'l' -> "l";
                            default -> throw new IllegalStateException("Unexpected value: " + value.iNodeType);
                        })
                        .name("permission").value(permissionStatus.getPermission().toString())
                        .endObject();
            }
        }

        @Override
        public Result read(JsonReader in) {
            throw new IllegalStateException("Not implemented/unused");
        }
    }

    record Report(Set<Result> results,
                  long fileCount,
                  long dirCount,
                  long symLinkCount) {
    }

    static class PathVisitor implements FsVisitor {
        final INodePredicate predicate;

        final PrintStream out;
        final FsImageData fsImageData;
        final LongAdder fileCount = new LongAdder();
        final LongAdder dirCount = new LongAdder();
        final LongAdder symLinkCount = new LongAdder();
        final Set<Result> results = new ConcurrentSkipListSet<>(Comparator.comparing(o -> o.path));

        PathVisitor(FsImageData fsImageData, PrintStream out, INodePredicate predicate) {
            this.fsImageData = fsImageData;
            this.out = out;
            this.predicate = predicate;
        }

        @Override
        public void onFile(INode inode, String path) {
            onInode(inode, path);
        }

        @Override
        public void onDirectory(INode inode, String path) {
            onInode(inode, path);
        }

        @Override
        public void onSymLink(INode inode, String path) {
            onInode(inode, path);
        }

        private void onInode(INode iNode, String path) {
            if (predicate.test(iNode)) {
                final String iNodeName = iNode.getName().toStringUtf8();
                final String absolutPath = path.length() > 1 ? path + '/' + iNodeName : path + iNodeName;
                char iNodeType = '-';
                if (iNode.hasFile()) {
                    fileCount.increment();
                } else if (iNode.hasDirectory()) {
                    iNodeType = 'd';
                    dirCount.increment();
                } else if (iNode.hasSymlink()) {
                    iNodeType = 'l';
                    symLinkCount.increment();
                }
                results.add(new Result(fsImageData.getPermission(iNode), absolutPath, iNodeType));
            }
        }
    }

    @Override
    public void run() {
        final FsImageData fsImageData = loadFsImage();
        try {
            createReport(fsImageData);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @FunctionalInterface
    public interface INodePredicate {
        boolean test(INode iNode);
    }

    private void createReport(FsImageData fsImageData) throws IOException {
        INodePredicate predicate = getPredicate(fsImageData);
        final PathVisitor visitor = new PathVisitor(fsImageData, mainCommand.out, predicate);
        final FsVisitor.Builder builder = new FsVisitor.Builder().parallel();
        for (String dir : mainCommand.dirs) {
            builder.visit(fsImageData,
                    visitor,
                    dir);
        }

        switch (mainCommand.outputFormat) {
            case json:
                doJsonReport(fsImageData, visitor);
                return;
            case csv:
                doCsvReport(visitor, fsImageData);
                return;
            case txt:
                doTxtReport(fsImageData, predicate, visitor);
                break;
        }
    }

    private void doTxtReport(FsImageData fsImageData, INodePredicate predicate, PathVisitor visitor) {
        mainCommand.out.println();
        final String title = "Path report (" +
                (mainCommand.dirs.length == 1 ? "path=" + mainCommand.dirs[0] : "paths=" + Arrays.toString(mainCommand.dirs))
                + ", " + predicate + ") :";
        mainCommand.out.println(title);
        mainCommand.out.println(FormatUtil.padRight('-', title.length()));

        mainCommand.out.println();
        final int fileCount = visitor.fileCount.intValue();
        final int dirCount = visitor.dirCount.intValue();
        final int symLinkCount = visitor.symLinkCount.intValue();
        mainCommand.out.println(fileCount + (fileCount == 1 ? " file, " : " files, ")
                + dirCount + (dirCount == 1 ? " directory and " : " directories and ")
                + symLinkCount + (symLinkCount == 1 ? " symlink" : " symlinks"));
        mainCommand.out.println();

        int maxUserNameLength = 0;
        int maxGroupNameLength = 0;
        for (Result result : visitor.results) {
            final PermissionStatus permissionStatus = fsImageData.getPermissionStatus(result.permission);
            maxUserNameLength = Math.max(maxUserNameLength, permissionStatus.getUserName().length());
            maxGroupNameLength = Math.max(maxGroupNameLength, permissionStatus.getGroupName().length());
        }

        for (Result result : visitor.results) {
            StringBuilder buf = new StringBuilder();
            final PermissionStatus permissionStatus = fsImageData.getPermissionStatus(result.permission);
            buf.append(result.iNodeType);
            buf.append(permissionStatus.getPermission().toString());
            buf.append(' ');
            buf.append(permissionStatus.getUserName());
            FormatUtil.padRight(buf, ' ', maxUserNameLength - permissionStatus.getUserName().length());
            buf.append(' ');
            buf.append(permissionStatus.getGroupName());
            FormatUtil.padRight(buf, ' ', maxGroupNameLength - permissionStatus.getGroupName().length());

            buf.append(' ');
            buf.append(result.path);

            mainCommand.out.println(buf);
        }
    }

    private void doJsonReport(FsImageData fsImageData, PathVisitor visitor) {
        final Report report = new Report(visitor.results,
                visitor.fileCount.longValue(),
                visitor.dirCount.longValue(),
                visitor.symLinkCount.longValue());
        GsonBuilder gsonBuilder = createGsonBuilder();
        gsonBuilder.registerTypeAdapter(Result.class, new ResultTypeAdapter(fsImageData));
        mainCommand.out.println(gsonBuilder.create().toJson(report));
    }

    private @NonNull INodePredicate getPredicate(FsImageData fsImageData) {
        INodePredicate predicate;
        if (null != mainCommand.userNameFilter) {
            predicate = new INodePredicate() {
                final Pattern userPattern = Pattern.compile(mainCommand.userNameFilter);

                @Override
                public String toString() {
                    return "user=~" + userPattern;
                }

                @Override
                public boolean test(INode iNode) {
                    final PermissionStatus permissionStatus = fsImageData.getPermissionStatus(iNode);
                    return userPattern.matcher(permissionStatus.getUserName()).matches();
                }
            };
        } else {
            predicate = new INodePredicate() {
                @Override
                public boolean test(INode iNode) {
                    return true;
                }

                @Override
                public String toString() {
                    return "no filter";
                }
            };
        }
        return predicate;
    }

    private void doCsvReport(PathVisitor visitor, FsImageData fsImageData) throws IOException {
        try (CSVPrinter printer = getCsvPrinter()) {
            printer.printRecord("Path", "Type","Permission");
            for (Result result : visitor.results) {
                printer.printRecord(result.path, result.iNodeType,
                        fsImageData.getPermissionStatus(result.permission));
            }
        }
    }

}
