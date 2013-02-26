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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.library.ExternalLibraryManager;
import edu.uci.ics.asterix.external.library.Function;
import edu.uci.ics.asterix.external.library.Functions;
import edu.uci.ics.asterix.external.library.Library;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataverse;

public class ExternalLibraryBootstrap {

    public static void setUpExternaLibraries(String nodeId, boolean installLibrary) throws Exception {

        File deployDir = getExternalLibraryDeployDir(nodeId);
        if (!deployDir.exists()) {
            return;
        }
        for (String libraryDirName : deployDir.list()) {
            File dataverseDir = new File(deployDir, libraryDirName);
            String dataverse = dataverseDir.getName();
            String[] libraries = dataverseDir.list();
            for (String library : libraries) {
                File libraryDir = new File(dataverseDir.getAbsolutePath() + File.separator + library);
                if (installLibrary) {
                    installLibrary(dataverseDir.getName(), libraryDir);
                }
                setupEnvironmentForExternalLibrary(nodeId, dataverse, libraryDirName);
            }
        }
    }

    private static void installLibrary(String dataverseName, File libraryDir) throws AsterixException {

        String[] libraryDescriptors = libraryDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return false;
            }
        });

        if (libraryDescriptors.length == 0) {
            throw new AsterixException("No library descriptors defined");
        } else if (libraryDescriptors.length > 1) {
            throw new AsterixException("More than 1 library descriptors defined");
        }

    }

    public static void setupEnvironmentForExternalLibrary(String nodeId, String dataverse, String libraryName)
            throws Exception {
        ClassLoader classLoader = getLibraryClassLoader(nodeId, dataverse);
        ExternalLibraryManager.registerLibraryClassLoader(dataverse, libraryName, classLoader);
    }

    private static Library getLibrary(File libraryXMLPath) throws Exception {
        JAXBContext configCtx = JAXBContext.newInstance(Library.class);
        Unmarshaller unmarshaller = configCtx.createUnmarshaller();
        Library library = (Library) unmarshaller.unmarshal(libraryXMLPath);
        return library;
    }

    // Each element of a library is installed as part of a transaction. Any failure in
    // installing an element does not effect installation of other components of the library or other libraries.
    private static void installLibrary(String dataverse, Library library) throws Exception {
        MetadataTransactionContext mdTxnCtx = null;
        for (Function function : library.getFunctions().getFunction()) {
            try {
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
                AqlMetadataProvider provider = new AqlMetadataProvider(mdTxnCtx, dv);

                String[] fargs = function.getArguments().trim().split(",");
                List<String> args = new ArrayList<String>();
                for (String arg : fargs) {
                    args.add(arg);
                }
                edu.uci.ics.asterix.metadata.entities.Function f = new edu.uci.ics.asterix.metadata.entities.Function(
                        dataverse, function.getName(), args.size(), args, function.getReturnType(),
                        function.getDefinition(), library.getLanguage(), function.getFunctionType());

                MetadataManager.INSTANCE.addFunction(mdTxnCtx, f);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            } catch (Exception e) {
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                throw e;
            }
        }
    }

    private static ClassLoader getLibraryClassLoader(String nodeId, String libraryDir) throws MalformedURLException {
        File deployDir = getExternalLibraryDeployDir(nodeId);
        File libDir = new File(deployDir, libraryDir + "/" + "/" + "lib");
        String[] libJars = libDir.list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            };
        });
        ClassLoader parentClassLoader = ExternalLibraryBootstrap.class.getClassLoader();
        URL[] urls = new URL[libJars.length];
        int count = 0;
        for (String dependency : libJars) {
            File file = new File(libDir.getAbsoluteFile() + "/" + dependency);
            urls[count++] = file.toURL();
        }
        ClassLoader classLoader = new URLClassLoader(urls, parentClassLoader);
        return classLoader;
    }

    private static File getExternalLibraryDeployDir(String nodeId) {
        String filePath = null;
        if (nodeId != null) {
            filePath = "edu.uci.ics.hyracks.control.nc.NodeControllerService" + "/" + nodeId + "/"
                    + "applications/asterix/expanded/external-lib/libraries";
        } else {
            filePath = "ClusterControllerService" + "/" + "applications/asterix/expanded/external-lib/libraries";

        }
        return new File(filePath);
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
