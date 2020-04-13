package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.m3y.hadoop.hdfs.hfsa.core.FsImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import de.m3y.hadoop.hdfs.hfsa.util.IECBinary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Abstract base class for report commands.
 */
abstract class AbstractReportCommand implements Runnable {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    @CommandLine.ParentCommand
    protected HdfsFSImageTool.MainCommand mainCommand;

    protected FsImageData loadFsImage() {
        try (RandomAccessFile file = new RandomAccessFile(mainCommand.fsImageFile, "r")) {
            if(log.isInfoEnabled()) {
                log.info("Starting loading {} of size {}", mainCommand.fsImageFile, IECBinary.format(file.length()));
            }

            // Warn about insufficient memory
            final long maxJvmMemory = Runtime.getRuntime().maxMemory();
            if (file.length() > maxJvmMemory) {
                mainCommand.out.println();
                mainCommand.out.println("Warning - Probably insufficient JVM max memory of " + IECBinary.format(maxJvmMemory));
                mainCommand.out.println("          Recommended heap for FSImage size of " + IECBinary.format(file.length()) +
                        " is " + IECBinary.format(file.length() * 2L));
                mainCommand.out.println("          Set JAVA_OPTS=\"-Xmx=...\"");
                mainCommand.out.println();
            }

            return new FsImageLoader.Builder().parallel().build().load(file);
        } catch (FileNotFoundException e) {
            mainCommand.err.println("No such fsimage file " + mainCommand.fsImageFile);
            throw new IllegalStateException("No such fsimage file " + mainCommand.fsImageFile, e);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
