package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
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
        try {
            showInodeDetails(fsImageData, inodeIds);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    sealed interface Result permits Success, Failure {
        String inodeIdOrPath();
    }

    record Success(String inodeIdOrPath, FsImageProto.INodeSection.INode iNode) implements Result {
    }

    record Failure(String inodeIdOrPath, String message) implements Result {
    }

    interface FormattingPrinter extends AutoCloseable {
        void print(Result result);

        @Override
        default void close() throws Exception {
            // Nothing
        }
    }

    private void showInodeDetails(FsImageData fsImageData, String[] inodeIdOrPaths) throws Exception {
        PrintStream out = mainCommand.out;
        try (FormattingPrinter printer = createPrinter(fsImageData, out)) {
            Arrays.stream(inodeIdOrPaths)
                    .map(inodeIdOrPath -> loadINode(fsImageData, inodeIdOrPath))
                    .forEach(printer::print);
        }
    }

    private Result loadINode(FsImageData fsImageData, String inodeIdOrPath) {
        try {
            try {
                long inodeIdAsLong = Long.parseLong(inodeIdOrPath);
                return new Success(inodeIdOrPath, fsImageData.getInode(inodeIdAsLong));
            } catch (NumberFormatException ex) {
                return new Success(inodeIdOrPath, fsImageData.getINodeFromPath(inodeIdOrPath));
            }
        } catch (FileNotFoundException e) {
            return new Failure(inodeIdOrPath,
                    "Can not find INode by id/path '" + inodeIdOrPath + "'");
        } catch (IllegalArgumentException | IOException e) {
            return new Failure(inodeIdOrPath,
                    "Can not find INode by id/path '" + inodeIdOrPath + "': " + e.getMessage());
        }
    }

    private InodeInfoCommand.FormattingPrinter createPrinter(FsImageData fsImageData, PrintStream out) {
        return switch (mainCommand.outputFormat) {
            case json -> createJsonPrinter(fsImageData, out);
            case csv -> createPrinter();
            case txt -> createTxtPrinter(out);
        };
    }

    private static InodeInfoCommand.FormattingPrinter createTxtPrinter(PrintStream out) {
        return result -> {
            if (result instanceof Success) {
                out.println(((Success) result).iNode.toString());
            } else if (result instanceof Failure) {
                out.println(((Failure) result).message);
            }
        };
    }

    private InodeInfoCommand.FormattingPrinter createPrinter() {
        CSVPrinter csvPrinter = getCsvPrinter();
        return new FormattingPrinter() {
            boolean first = true;

            @Override
            public void close() throws IOException {
                csvPrinter.close();
            }

            @Override
            public void print(Result result) {
                try {
                    if (first) {
                        csvPrinter.printRecord("ID", "Name", "Type");
                        first = false;
                    }
                    if (result instanceof Success) {
                        FsImageProto.INodeSection.INode iNode = ((Success) result).iNode;
                        csvPrinter.printRecord(iNode.getId(), iNode.getName().toStringUtf8(), iNode.getType());
                    } else if (result instanceof Failure) {
                        csvPrinter.printRecord(result.inodeIdOrPath(), "- ERR -", "- ERR -");
                        mainCommand.err.println(((Failure) result).message());
                    }
                } catch (IOException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        };
    }

    private InodeInfoCommand.FormattingPrinter createJsonPrinter(FsImageData fsImageData, PrintStream out) {
        GsonBuilder gsonBuilder = createGsonBuilder();
        gsonBuilder.registerTypeAdapter(FsImageProto.INodeSection.INode.class,
                new JsonUtil.INodeTypeAdapter(fsImageData));
        Gson gson = gsonBuilder.create();
        try {
            JsonWriter jsonWriter = gson.newJsonWriter(new PrintWriter(out))
                    .beginObject().name("results").beginArray();
            return new FormattingPrinter() {
                @Override
                public void print(Result result) {
                    try {
                        if (result instanceof Success) {
                            FsImageProto.INodeSection.INode iNode = ((Success) result).iNode;
                            jsonWriter.beginObject()
                                    .name("inode_arg").value(result.inodeIdOrPath())
                                    .name("inode");
                            gson.toJson(iNode, iNode.getClass(), jsonWriter);
                            jsonWriter.endObject();
                        } else if (result instanceof Failure) {
                            jsonWriter.beginObject()
                                    .name("inode_arg").value(result.inodeIdOrPath())
                                    .name("error").value(((Failure) result).message)
                                    .endObject();
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }

                @Override
                public void close() throws IOException {
                    jsonWriter.endArray().endObject();
                    jsonWriter.close();
                }
            };
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
