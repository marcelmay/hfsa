package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.File;

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

    @CommandLine.Option(names = {"-s", "--sort"}, help = true,
            description = "Sort by <fs> size (default), <fc> file count, <dc> directory count or <bc> block count.")
    String sort = "fs";

    @CommandLine.Option(names = {"-fun", "--filter-by-user"}, help = true,
            description = "Filter user name by <regexp>.")
    String userNameFilter;

    @CommandLine.Parameters(arity = "1", paramLabel = "FILE", index = "0",
            description = "FSImage file to process.")
    File fsImageFile;

    @CommandLine.Parameters(paramLabel = "DIRS", index="1..*",
            description = "Directory paths to start traversing.")
    String[] dirs = new String[]{"/"};
}
