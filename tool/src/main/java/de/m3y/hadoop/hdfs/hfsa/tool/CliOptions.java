package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.File;
import java.util.Comparator;

import picocli.CommandLine;

/**
 * Command line options for tool
 */
@CommandLine.Command(name = "hfsa-tool", separator = " ", showDefaultValues = true)
class CliOptions {
    @CommandLine.Option(names = {"-h", "--help"}, help = true,
            description = "Displays this help message and quits.")
    boolean helpRequested = false;

    @CommandLine.Option(names = "-v",
            description = "Turns on verbose output. Use `-vv` for debug output."
    )
    boolean[] verbose;

    /**
     * Sort options.
     */
    enum SortOption {
        /**
         * file size
         */
        fs(HdfsFSImageTool.AbstractStats.COMPARATOR_SUM_FILE_SIZE),
        /**
         * file count
         */
        fc(HdfsFSImageTool.AbstractStats.COMPARATOR_SUM_FILES),
        /**
         * directory count
         */
        dc(HdfsFSImageTool.AbstractStats.COMPARATOR_SUM_DIRECTORIES),
        /**
         * block count
         */
        bc(HdfsFSImageTool.AbstractStats.COMPARATOR_BLOCKS);

        private final Comparator<HdfsFSImageTool.AbstractStats> comparator;

        SortOption(Comparator<HdfsFSImageTool.AbstractStats> comparator) {
            this.comparator = comparator;
        }

        public Comparator<HdfsFSImageTool.AbstractStats> getComparator() {
            return comparator;
        }}

    @CommandLine.Option(names = {"-s", "--sort"}, help = true,
            description = "Sort by <fs> size, <fc> file count, <dc> directory count or <bc> block count (default: ${DEFAULT-VALUE}). ")
    SortOption sort = SortOption.fs;

    @CommandLine.Option(names = {"-fun", "--filter-by-user"}, help = true,
            description = "Filter user name by <regexp>.")
    String userNameFilter;

    @CommandLine.Parameters(arity = "1", paramLabel = "FILE", index = "0",
            description = "FSImage file to process.")
    File fsImageFile;

    @CommandLine.Parameters(paramLabel = "DIRS", index = "1..*",
            description = "Directory path(s) to start traversing (default: ${DEFAULT-VALUE}).")
    String[] dirs = new String[]{"/"};
}
