/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.installer.command;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.service.ILookupService;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class InstallCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig();
        InstallConfig installConfig = ((InstallConfig) config);
        String instanceName = installConfig.name;
        InstallerUtil.validateAsterixInstanceExists(instanceName, State.INACTIVE);
        ILookupService lookupService = ServiceProvider.INSTANCE.getLookupService();
        AsterixInstance instance = lookupService.getAsterixInstance(instanceName);
        PatternCreator pc = new PatternCreator();
        Patterns patterns = pc.getLibraryInstallPattern(instance.getCluster(), installConfig.dataverseName,
                installConfig.libraryName, installConfig.libraryPath);
        InstallerUtil.getEventrixClient(instance.getCluster()).submit(patterns);
        LOGGER.info("Installed library " + installConfig.libraryName);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new InstallConfig();
    }

    @Override
    protected String getUsageDescription() {
        // TODO Auto-generated method stub
        return null;
    }

}

class InstallConfig extends AbstractCommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-d", required = true, usage = "Name of the dataverse under which the library will be installed")
    public String dataverseName;

    @Option(name = "-name", required = true, usage = "Name of the library")
    public String libraryName;

    @Option(name = "-p", required = true, usage = "Path to library zip bundle")
    public String libraryPath;

}
