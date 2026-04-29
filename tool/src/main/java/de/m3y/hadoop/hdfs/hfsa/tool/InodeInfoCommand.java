package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.IOException;
import java.io.PrintStream;

import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import picocli.CommandLine;

/**
 * Reports details about an inode structure.
 */
@CommandLine.Command(name = "inode", aliases = "i",
        description = "Shows INode details",
        mixinStandardHelpOptions = true,
        helpCommand = true,
        showDefaultValues = true
)
public class InodeInfoCommand extends AbstractReportCommand {
    @CommandLine.Parameters(paramLabel = "INODES", arity = "1..*",
            description = "At least one INode id, eg ROOT inode " + INodeId.ROOT_INODE_ID + " or absolute path like '/foo/bar.txt'.")
    String[] inodeIds = new String[0];

    @Override
    public void run() {
        final FsImageData fsImageData = loadFsImage();
        if (null != fsImageData) {
            for (String inodeId : inodeIds) {
                showInodeDetails(fsImageData, inodeId);
            }
        }

    }

    private void showInodeDetails(FsImageData fsImageData, String inodeId) {
        PrintStream out = mainCommand.out;
        try {
            final FsImageProto.INodeSection.INode inode = loadInode(fsImageData, inodeId);
            if (isJson()) {
                out.println(getGson().toJson(inode));
            } else if (isCsv()) {
                try (CSVPrinter printer = getCsvPrinter()) {
                    printer.printRecord("ID", "Name", "Type");
                    printer.printRecord(inode.getId(), inode.getName().toStringUtf8(), inode.getType());
                }
            } else {
                out.println(inode.toString());
            }
        } catch (IOException e) {
            out.println("Can not find INode by id/path " + inodeId);
        }
    }

    private FsImageProto.INodeSection.INode loadInode(FsImageData fsImageData, String inodeId) throws IOException {
        try {
            long inodeIdAsLong = Long.parseLong(inodeId);
            return fsImageData.getInode(inodeIdAsLong);
        } catch (NumberFormatException ex) {
            return fsImageData.getINodeFromPath(inodeId);
        }
    }
}
