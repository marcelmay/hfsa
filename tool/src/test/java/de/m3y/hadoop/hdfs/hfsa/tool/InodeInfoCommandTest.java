package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class InodeInfoCommandTest {
    @Test
    public void testRun() {
        InodeInfoCommand infoCommand = new InodeInfoCommand();
        infoCommand.mainCommand = new HdfsFSImageTool.MainCommand();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (PrintStream printStream = new PrintStream(byteArrayOutputStream)) {
            infoCommand.mainCommand.out = printStream;
            infoCommand.mainCommand.err = infoCommand.mainCommand.out;

            infoCommand.mainCommand.fsImageFile = new File("src/test/resources/fsi_small.img");
            infoCommand.inodeIds = new String[]{"/", "/test3", "/test3/test_160MiB.img", "16387"};
            infoCommand.run();

            assertThat(byteArrayOutputStream.toString())
                    .isEqualTo(
                            "type: DIRECTORY\n" +
                                    "id: 16385\n" +
                                    "name: \"\"\n" +
                                    "directory {\n" +
                                    "  modificationTime: 1499493618390\n" +
                                    "  nsQuota: 9223372036854775807\n" +
                                    "  dsQuota: 18446744073709551615\n" +
                                    "  permission: 1099511759341\n" +
                                    "}\n" +
                                    "\n" +
                                    "type: DIRECTORY\n" +
                                    "id: 16388\n" +
                                    "name: \"test3\"\n" +
                                    "directory {\n" +
                                    "  modificationTime: 1497734744891\n" +
                                    "  nsQuota: 18446744073709551615\n" +
                                    "  dsQuota: 18446744073709551615\n" +
                                    "  permission: 1099511759341\n" +
                                    "}\n" +
                                    "\n" +
                                    "type: FILE\n" +
                                    "id: 16402\n" +
                                    "name: \"test_160MiB.img\"\n" +
                                    "file {\n" +
                                    "  replication: 1\n" +
                                    "  modificationTime: 1497734744886\n" +
                                    "  accessTime: 1497734743534\n" +
                                    "  preferredBlockSize: 134217728\n" +
                                    "  permission: 5497558401444\n" +
                                    "  blocks {\n" +
                                    "    blockId: 1073741834\n" +
                                    "    genStamp: 1010\n" +
                                    "    numBytes: 134217728\n" +
                                    "  }\n" +
                                    "  blocks {\n" +
                                    "    blockId: 1073741835\n" +
                                    "    genStamp: 1011\n" +
                                    "    numBytes: 33554432\n" +
                                    "  }\n" +
                                    "  storagePolicyID: 0\n" +
                                    "}\n" +
                                    "\n" +
                                    "type: DIRECTORY\n" +
                                    "id: 16387\n" +
                                    "name: \"test2\"\n" +
                                    "directory {\n" +
                                    "  modificationTime: 1497733426149\n" +
                                    "  nsQuota: 18446744073709551615\n" +
                                    "  dsQuota: 18446744073709551615\n" +
                                    "  permission: 1099511759341\n" +
                                    "}\n" +
                                    "\n"
                    );
        }
    }

}
