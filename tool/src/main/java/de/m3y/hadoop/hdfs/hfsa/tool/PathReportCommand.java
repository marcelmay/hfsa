package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
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

    static class Result {
        final long permission; // user, group, FS permissions
        final String path;
        final char iNodeType;

        Result(long permission, String path, char iNodeType) {
            this.permission = permission;
            this.path = path;
            this.iNodeType = iNodeType;
        }
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
            if (predicate.test(iNode, path)) {
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
        if (null != fsImageData) {
            createReport(fsImageData);
        }
    }

    @FunctionalInterface
    public interface INodePredicate {
        boolean test(INode iNode, String path);
    }

    private void createReport(FsImageData fsImageData) {
        try {
            INodePredicate predicate;
            if (null != mainCommand.userNameFilter) {
                Pattern userPattern = Pattern.compile(mainCommand.userNameFilter);
                predicate = new INodePredicate() {
                    @Override
                    public String toString() {
                        return "user=~" + userPattern;
                    }

                    @Override
                    public boolean test(INode iNode, String path) {
                        final PermissionStatus permissionStatus = fsImageData.getPermissionStatus(iNode);
                        return userPattern.matcher(permissionStatus.getUserName()).matches();
                    }
                };
            } else {
                predicate = new INodePredicate() {
                    @Override
                    public boolean test(INode iNode, String path) {
                        return true;
                    }

                    @Override
                    public String toString() {
                        return "no filter";
                    }
                };
            }
            final PathVisitor visitor = new PathVisitor(fsImageData, mainCommand.out, predicate);
            final FsVisitor.Builder builder = new FsVisitor.Builder().parallel();
            for (String dir : mainCommand.dirs) {
                builder.visit(fsImageData,
                        visitor,
                        dir);
            }

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

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


}
