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

            assertThat(byteArrayOutputStream)
                    .hasToString(
                            """
                                    type: DIRECTORY
                                    id: 16385
                                    name: ""
                                    directory {
                                      modificationTime: 1499493618390
                                      nsQuota: 9223372036854775807
                                      dsQuota: 18446744073709551615
                                      permission: 1099511759341
                                    }
                                    
                                    type: DIRECTORY
                                    id: 16388
                                    name: "test3"
                                    directory {
                                      modificationTime: 1497734744891
                                      nsQuota: 18446744073709551615
                                      dsQuota: 18446744073709551615
                                      permission: 1099511759341
                                    }
                                    
                                    type: FILE
                                    id: 16402
                                    name: "test_160MiB.img"
                                    file {
                                      replication: 1
                                      modificationTime: 1497734744886
                                      accessTime: 1497734743534
                                      preferredBlockSize: 134217728
                                      permission: 5497558401444
                                      blocks {
                                        blockId: 1073741834
                                        genStamp: 1010
                                        numBytes: 134217728
                                      }
                                      blocks {
                                        blockId: 1073741835
                                        genStamp: 1011
                                        numBytes: 33554432
                                      }
                                      storagePolicyID: 0
                                    }
                                    
                                    type: DIRECTORY
                                    id: 16387
                                    name: "test2"
                                    directory {
                                      modificationTime: 1497733426149
                                      nsQuota: 18446744073709551615
                                      dsQuota: 18446744073709551615
                                      permission: 1099511759341
                                    }
                                    
                                    """
                    );
        }
    }

}
