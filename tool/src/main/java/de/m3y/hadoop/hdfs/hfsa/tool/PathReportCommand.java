package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

import de.m3y.hadoop.hdfs.hfsa.core.*;
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
        final String user;
        final String group;
        final String permission;
        final String path;
        final char iNodeType;

        Result(String user, String group, String permission, String path, char iNodeType) {
            this.user = user;
            this.group = group;
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
        public void onFile(FsImageFile fsImageFile) {
            onFsImageInfo(fsImageFile);
        }

        @Override
        public void onDirectory(FsImageDir fsImageDir) {
            onFsImageInfo(fsImageDir);
        }

        @Override
        public void onSymLink(FsImageSymLink fsImageSymLink) {
            onFsImageInfo(fsImageSymLink);
        }

        private void onFsImageInfo(FsImageInfo fsImageInfo) {
            String path  = fsImageInfo.getPath();
            if (predicate.test(fsImageInfo)) {
                char iNodeType = '-';
                if (fsImageInfo instanceof FsImageFile) {
                    fileCount.increment();
                } else if (fsImageInfo instanceof FsImageDir) {
                    iNodeType = 'd';
                    dirCount.increment();
                } else if (fsImageInfo instanceof FsImageSymLink) {
                    iNodeType = 'l';
                    symLinkCount.increment();
                }

                results.add(
                        new Result(fsImageInfo.getUser(), fsImageInfo.getGroup(), fsImageInfo.getPermission(),
                                path, iNodeType)
                );
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
        boolean test(FsImageInfo fsImageInfo);
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
                    public boolean test(FsImageInfo fsImageInfo) {
                        final String user = fsImageInfo.getUser();
                        return userPattern.matcher(user).matches();
                    }
                };
            } else {
                predicate = new INodePredicate() {
                    @Override
                    public boolean test(FsImageInfo fsImageInfo) {
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
                maxUserNameLength = Math.max(maxUserNameLength, result.user.length());
                maxGroupNameLength = Math.max(maxGroupNameLength, result.group.length());
            }

            for (Result result : visitor.results) {
                StringBuilder buf = new StringBuilder();
                buf.append(result.iNodeType);
                buf.append(result.permission);
                buf.append(' ');
                buf.append(result.user);
                FormatUtil.padRight(buf, ' ', maxUserNameLength - result.user.length());
                buf.append(' ');
                buf.append(result.group);
                FormatUtil.padRight(buf, ' ', maxGroupNameLength - result.group.length());

                buf.append(' ');
                buf.append(result.path);

                mainCommand.out.println(buf);
            }

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


}
