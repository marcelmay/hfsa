package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.*;

import static org.apache.log4j.Logger.getRootLogger;

/**
 * HDFS FSImage Tool extracts a summary of HDFS Usage from fsimage.
 */
public class HdfsFSImageTool {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsFSImageTool.class);

    /**
     * Generic options shared by all commands, like fsimage file or verbosity.
     */
    abstract static class BaseCommand implements Runnable {
        @Option(names = "-v", description = "Turns on verbose output. Use `-vv` for debug output.",
                scope = ScopeType.INHERIT) // option is shared with subcommands
        public void setVerbose(boolean[] verbose) {
            final org.apache.log4j.Logger rootLogger = getRootLogger();
            if (null == verbose) {
                rootLogger.setLevel(Level.WARN);
            } else {
                if (verbose.length == 1) {
                    rootLogger.setLevel(Level.INFO);
                } else {
                    rootLogger.setLevel(Level.DEBUG);
                    LOG.debug("Debug logging enabled");
                }
            }
        }

        @Parameters(paramLabel = "FILE", arity = "1",
                description = "FSImage file to process.")
        File fsImageFile;

        @Option(names = {"-p", "--path"},
                split = ",",
                description = "Directory path(s) to start traversing (default: ${DEFAULT-VALUE}).")
        String[] dirs = new String[]{"/"};

        @Option(names = {"-fun", "--filter-by-user"},
                description = "Filter user name by <regexp>.")
        String userNameFilter;
    }

    @Command(name = "hfsa-tool",
            header = "Analyze Hadoop FSImage file for user/group reports",
            footer = "Runs @|bold summary|@ command by default.",
            mixinStandardHelpOptions = true,
            versionProvider = VersionProvider.class,
            showDefaultValues = true,
            subcommands = {
                    SummaryReportCommand.class,
                    SmallFilesReportCommand.class,
                    InodeInfoCommand.class,
                    PathReportCommand.class,
                    UserUsageReportCommand.class
            }
    )
    static class MainCommand extends BaseCommand {
        PrintStream out = HdfsFSImageTool.out;
        PrintStream err = HdfsFSImageTool.err;

        @Override
        public void run() {
            // Default main command is the default summary
            SummaryReportCommand summaryReport = new SummaryReportCommand();
            summaryReport.mainCommand = this;
            summaryReport.run();
        }
    }

    static PrintStream out = System.out; // NOSONAR
    static PrintStream err = System.err; // NOSONAR

    protected static int run(String[] args) {
        final IExecutionExceptionHandler exceptionHandler = (ex, commandLine, parseResult) -> {
            String message = null == ex.getMessage() ? "" : ex.getMessage();
            commandLine.getErr().println(commandLine.getColorScheme().errorText(message));
            if (getRootLogger().isInfoEnabled()) {
                commandLine.getErr().println("Exiting - use option [-v] for more verbose details.");
            }
            if (getRootLogger().isDebugEnabled()) {
                commandLine.getErr().println(commandLine.getColorScheme().errorText(message));
            }
            return commandLine.getExitCodeExceptionMapper() != null
                    ? commandLine.getExitCodeExceptionMapper().getExitCode(ex)
                    : commandLine.getCommandSpec().exitCodeOnExecutionException();
        };

        CommandLine cmd = new CommandLine(new MainCommand());
        cmd.setColorScheme(Help.defaultColorScheme(Help.Ansi.AUTO));
        cmd.setOut(new PrintWriter(out));
        cmd.setExecutionExceptionHandler(exceptionHandler);
        cmd.setExecutionStrategy(new RunLast());
        return cmd.execute(args);
    }

    public static void main(String[] args) {
        System.exit(run(args));
    }
}
