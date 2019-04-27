package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
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
            return FSImageLoader.load(file);
        } catch (FileNotFoundException e) {
            mainCommand.err.println("No such fsimage file " + mainCommand.fsImageFile);
            throw new IllegalStateException("No such fsimage file " + mainCommand.fsImageFile, e);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
