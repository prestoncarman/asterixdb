package edu.uci.ics.asterix.installer.driver;

import java.io.File;
import java.io.FilenameFilter;

public class AsterixInstallerIntegrationUtil {

    private static String managixHome;

    public static void init() throws Exception {
        File installerProjectDir = new File(System.getProperty("user.dir"));
        File installerProjectTargetDir = new File(installerProjectDir, "target");
        File managixZipDir = new File(installerProjectTargetDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                File candidate = new File(dir, name);
                return candidate.isDirectory() && name.startsWith("asterix-installer")
                        && name.endsWith("binary-assembly");
            }
        })[0]);

        // asterix-installer/target/asterix-installer-0.0.5-SNAPSHOT-binary-assembly
        managixHome = installerProjectTargetDir.getAbsolutePath() + File.separator + managixZipDir;

    }

    public static String getManagixHome() {
        return managixHome;
    }

    public static void deinit() {
        // TODO Auto-generated method stub

    }
}
