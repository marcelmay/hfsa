package de.m3y.hadoop.hdfs.hfsa.tool;

import picocli.CommandLine;

/**
 * Holds compile-time injected version info for PicoCLI.
 */
class VersionProvider implements CommandLine.IVersionProvider {
    public String getAppVersion() {
        return "<injected during build>"; // NOSONAR
    }

    public String getBuildTimeStamp() {
        return "<injected during build>"; // NOSONAR
    }

    public String getBuildScmVersion() {
        return "<injected during build>"; // NOSONAR
    }

    public String getBuildScmBranch() {
        return "<injected during build>"; // NOSONAR
    }

    @Override
    public String[] getVersion() {
        return new String[]{
                "Version " + getAppVersion(),
                "Build timestamp " + getBuildTimeStamp(),
                "SCM Version " + getBuildScmVersion(),
                "SCM Branch " + getBuildScmBranch()
        };
    }
}
