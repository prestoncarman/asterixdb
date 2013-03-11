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
package edu.uci.ics.asterix.hyracks.bootstrap;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.external.library.ExternalLibraryManager;
import edu.uci.ics.asterix.external.library.Function;
import edu.uci.ics.asterix.external.library.Functions;
import edu.uci.ics.asterix.external.library.Library;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.runtime.formats.NonTaggedDataFormat;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;

public class ExternalLibraryBootstrap {

    public static void setUpExternaLibraries(boolean isMetadataNode) throws Exception {
        File installedLibDir = getInstalledLibraryDir();
        if (installedLibDir.exists()) {
            for (String dataverse : installedLibDir.list()) {
                File dataverseDir = new File(installedLibDir, dataverse);
                String[] libraries = dataverseDir.list();
                for (String library : libraries) {
                    System.out.println(" registering library " + dataverse + ":" + library);
                    registerLibrary(dataverse, library);
                }
            }
        }

        if (isMetadataNode) {
            if (!installedLibDir.exists()) {
                installedLibDir.mkdirs();
            }
            installLibraries();
            uninstallLibraries();
        }

    }

    private static void uninstallLibraries() throws Exception {
        File uninstallLibDir = getLibraryUninstallDir();
        String[] uninstallLibNames;
        if (uninstallLibDir.exists()) {
            uninstallLibNames = uninstallLibDir.list();
            for (String uninstallLibName : uninstallLibNames) {
                String[] components = uninstallLibName.split("\\.");
                String dataverse = components[0];
                String libName = components[1];
                uninstallLibrary(dataverse, libName);
                new File(uninstallLibDir + File.separator + uninstallLibName).delete();
                File f = new File(getInstalledLibraryDir() + File.separator + dataverse + File.separator + libName);
                if (f.exists()) {
                    f.delete();
                }
            }
        }
    }

    private static void uninstallLibrary(String dataverse, String libraryName) throws RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                return;
            }

            List<edu.uci.ics.asterix.metadata.entities.Function> functions = MetadataManager.INSTANCE
                    .getDataverseFunctions(mdTxnCtx, dataverse);
            for (edu.uci.ics.asterix.metadata.entities.Function function : functions) {
                if (function.getName().startsWith(libraryName + ":")) {
                    MetadataManager.INSTANCE.dropFunction(mdTxnCtx, new FunctionSignature(dataverse,
                            function.getName(), function.getArity()));
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
        }
    }

    private static void installLibraries() throws Exception {
        File installDir = getLibraryInstallDir();
        if (!installDir.exists()) {
            return;
        }
        for (String dataverse : installDir.list()) {
            File dataverseDir = new File(installDir, dataverse);
            String[] libraries = dataverseDir.list();
            for (String library : libraries) {
                File libraryDir = new File(dataverseDir, library);
                try {
                    installLibrary(dataverse, libraryDir);
                    registerLibrary(dataverse, library);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new Exception("Exception in installing library:" + library
                            + " followed by failure in rolling back");
                }
            }
        }
    }

    // Each element of a library is installed as part of a transaction. Any
    // failure in installing an element does not effect installation of other libraries
    private static void installLibrary(String dataverse, final File libraryDir) throws Exception {
        String[] libraryDescriptors = libraryDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".xml");
            }
        });

        if (libraryDescriptors.length == 0) {
            throw new Exception("No library descriptors defined");
        } else if (libraryDescriptors.length > 1) {
            throw new Exception("More than 1 library descriptors defined");
        }

        Library library = getLibrary(new File(libraryDir + File.separator + libraryDescriptors[0]));
        String libraryName = libraryDir.getName();

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                MetadataManager.INSTANCE.addDataverse(mdTxnCtx, new Dataverse(dataverse,
                        NonTaggedDataFormat.NON_TAGGED_DATA_FORMAT));
            }
            for (Function function : library.getFunctions().getFunction()) {
                String[] fargs = function.getArguments().trim().split(",");
                List<String> args = new ArrayList<String>();
                for (String arg : fargs) {
                    args.add(arg);
                }
                edu.uci.ics.asterix.metadata.entities.Function f = new edu.uci.ics.asterix.metadata.entities.Function(
                        dataverse, libraryName + ":" + function.getName(), args.size(), args, function.getReturnType(),
                        function.getDefinition(), library.getLanguage(), function.getFunctionType());
                MetadataManager.INSTANCE.addFunction(mdTxnCtx, f);
            }

            Runtime.getRuntime().exec(
                    "mv" + " " + libraryDir.getParentFile().getAbsolutePath() + " " + getInstalledLibraryDir()
                            + File.separator);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
        }
    }

    private static void registerLibrary(String dataverse, String libraryName) throws Exception {
        ClassLoader classLoader = getLibraryClassLoader(dataverse, libraryName);
        ExternalLibraryManager.registerLibraryClassLoader(dataverse, libraryName, classLoader);
    }

    private static void deregisterLibrary(String dataverse, String libraryName) throws Exception {
        ExternalLibraryManager.deregisterLibraryClassLoader(dataverse, libraryName);
    }

    private static Library getLibrary(File libraryXMLPath) throws Exception {
        JAXBContext configCtx = JAXBContext.newInstance(Library.class);
        Unmarshaller unmarshaller = configCtx.createUnmarshaller();
        Library library = (Library) unmarshaller.unmarshal(libraryXMLPath);
        return library;
    }

    private static ClassLoader getLibraryClassLoader(String dataverse, String libraryName) throws Exception {
        File installDir = getInstalledLibraryDir();
        File libDir = new File(installDir.getAbsolutePath() + File.separator + dataverse + File.separator + libraryName);
        FilenameFilter jarFileFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        };

        System.out.println(" looking for jar in " + libDir.getAbsolutePath());

        String[] jarsInLibDir = libDir.list(jarFileFilter);
        if (jarsInLibDir.length > 1) {
            throw new Exception("Incorrect library structure: found multiple library jars");
        }
        if (jarsInLibDir.length < 0) {
            throw new Exception("Incorrect library structure: could not find library jar");
        }

        File libJar = new File(libDir, jarsInLibDir[0]);
        File libDependencyDir = new File(libDir.getAbsolutePath() + File.separator + "lib");
        int numDependencies = 1;
        String[] libraryDependencies = null;
        if (libDependencyDir.exists()) {
            libraryDependencies = libDependencyDir.list(jarFileFilter);
            numDependencies += libraryDependencies.length;
        }

        ClassLoader parentClassLoader = ExternalLibraryBootstrap.class.getClassLoader();
        URL[] urls = new URL[numDependencies];
        int count = 0;
        urls[count++] = libJar.toURL();

        if (libraryDependencies != null && libraryDependencies.length > 0) {
            for (String dependency : libraryDependencies) {
                File file = new File(libDependencyDir + File.separator + dependency);
                urls[count++] = file.toURL();
            }
        }
        ClassLoader classLoader = new URLClassLoader(urls, parentClassLoader);
        return classLoader;
    }

    private static File getLibraryInstallDir() {
        String workingDir = System.getProperty("user.dir");
        return new File(workingDir + File.separator + "install");
    }

    private static File getLibraryUninstallDir() {
        String workingDir = System.getProperty("user.dir");
        return new File(workingDir + File.separator + "uninstall");
    }

    private static File getInstalledLibraryDir() {
        String workingDir = System.getProperty("user.dir");
        return new File(workingDir + File.separator + "installed");
    }

}

class ExternalLibrary {

    private final String dataverse;
    private final String name;
    private final String language;
    private final Functions functions;

    public ExternalLibrary(String dataverse, String name, String language, Functions functions) {
        this.dataverse = dataverse;
        this.name = name;
        this.language = language;
        this.functions = functions;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder("");
        builder.append("Library");
        builder.append("\n");
        builder.append("Functions");
        builder.append("\n");
        for (Function function : functions.getFunction()) {
            builder.append(function);
            builder.append("\n");
        }
        return new String(builder);
    }

    public String getDataverse() {
        return dataverse;
    }

    public String getName() {
        return name;
    }

    public String getLanguage() {
        return language;
    }

    public Functions getFunctions() {
        return functions;
    }

}
