package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
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

    protected FSImageLoader loadFsImage() {
        try {
            RandomAccessFile file = new RandomAccessFile(mainCommand.fsImageFile, "r");
            log.info("Starting loading " + mainCommand.fsImageFile + " of size " + IECBinary.format(file.length()));
            if (file.length() > Runtime.getRuntime().totalMemory()) {
                mainCommand.out.println();
                mainCommand.out.println("Warning - Probably insufficient JVM max memory of "+IECBinary.format(Runtime.getRuntime().totalMemory()));
                mainCommand.out.println("          Recommended heap for FSImage size of " + IECBinary.format(file.length()) +
                        " is " + IECBinary.format(file.length() * 2L));
                mainCommand.out.println("          Set JAVA_OPTS=\"-Xmx=...\"");
                mainCommand.out.println();
            }
            return FSImageLoader.load(file);
        } catch (FileNotFoundException e) {
            mainCommand.err.println("No such fsimage file " + mainCommand.fsImageFile);
            throw new IllegalStateException("No such fsimage file " + mainCommand.fsImageFile, e);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
