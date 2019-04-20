package de.m3y.hadoop.hdfs.hfsa.tool;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.spi.RootLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * HDFS FSImage Tool extracts a summary of HDFS Usage from fsimage.
 */
public class HdfsFSImageTool {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsFSImageTool.class);

    /**
     * Generic options shared by all commands, like fsimage file or verbosity.
     */
    abstract static class BaseCommand implements Runnable {
        @Option(names = "-v",
                description = "Turns on verbose output. Use `-vv` for debug output."
        )
        boolean[] verbose = new boolean[0];

        @Parameters(paramLabel = "FILE",  arity = "1",
                description = "FSImage file to process.")
        File fsImageFile;

        @Option(names = {"-p","--path"},
                split = ",",
                description = "Directory path(s) to start traversing (default: ${DEFAULT-VALUE}).")
        String[] dirs = new String[]{"/"};

        @CommandLine.Option(names = {"-fun", "--filter-by-user"},
                description = "Filter user name by <regexp>.")
        String userNameFilter;
    }

    @CommandLine.Command(name = "hfsa-tool",
            header = "Analyze Hadoop FSImage file for user/group reports",
            footer = "Runs @|bold summary|@ command by default.",
            mixinStandardHelpOptions = true,
            versionProvider = VersionProvider.class,
            showDefaultValues = true,
            subcommands = {SummaryReportCommand.class}
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

    public static void main(String[] args) {
        final CommandLine.Help.Ansi ansi = CommandLine.Help.Ansi.AUTO;
        final CommandLine.AbstractParseResultHandler<List<Object>> handler =
                new CommandLine.RunLast().useOut(out).useAnsi(ansi);
        final CommandLine.DefaultExceptionHandler<List<Object>> exceptionHandler =
                new CommandLine.DefaultExceptionHandler<List<Object>>().useErr(err).useAnsi(ansi);

        CommandLine cmd = new CommandLine(new MainCommand());
        CommandLine.ParseResult parseResult = null;
        try {
            parseResult = cmd.parseArgs(args);

            final CommandLine.Model.OptionSpec verbose = parseResult.matchedOption("v");
            if (null != verbose) {
                handleVerboseMode(verbose);
            }

            handler.handleParseResult(parseResult);

        } catch (CommandLine.ParameterException ex) {
            exceptionHandler.handleParseException(ex, args);
        } catch (CommandLine.ExecutionException ex) {
            exceptionHandler.handleExecutionException(ex, parseResult);
        }
    }

    private static void handleVerboseMode(CommandLine.Model.OptionSpec verbose) {
        if (null == verbose) {
            RootLogger.getRootLogger().setLevel(Level.WARN);
        } else {
            boolean[] values = verbose.getValue();
            if (null != values) {
                if (values.length == 1) {
                    RootLogger.getRootLogger().setLevel(Level.INFO);
                } else {
                    RootLogger.getRootLogger().setLevel(Level.DEBUG);
                    LOG.debug("Debug logging enabled");
                }
            }
        }
    }
}
