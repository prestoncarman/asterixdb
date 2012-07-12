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
package edu.uci.ics.asterix.metadata.bootstrap;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.uci.ics.asterix.external.dataset.adapter.AdapterIdentifier;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Adapter;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.runtime.external.ExternalLibraryManager;
import edu.uci.ics.asterix.runtime.formats.NonTaggedDataFormat;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;

public class ExternalLibraryBootstrap {

    public static void setUpExternaLibraries(String nodeId, boolean installLibrary) throws Exception {

        File deployDir = ExternalLibraryManager.getExternalLibraryDeployDir(nodeId);
        if (!deployDir.exists()) {
            return;
        }
        for (String libraryDirName : deployDir.list()) {
            File libraryDir = new File(deployDir, libraryDirName);
            String[] libraryXMLs = libraryDir.list(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.endsWith(".xml");
                };
            });
            if (libraryXMLs.length > 1) {
                throw new IllegalStateException(" More than one library descriptor found");
            }

            Library library = getLibrary(new File(libraryDir, libraryXMLs[0]));
            if (installLibrary) {
                installLibrary(library);
            }
            setupEnvironmentForExternalLibrary(nodeId, libraryDirName, library);
        }
    }

    public static void setupEnvironmentForExternalLibrary(String nodeId, String libraryDir, Library library)
            throws Exception {
        ClassLoader classLoader = getLibraryClassLoader(nodeId, libraryDir, library);
        ExternalLibraryManager.registerLibraryClassLoader(library.getDataverse(), library.getName(), classLoader);
    }

    private static Library getLibrary(File libraryXMLPath) throws Exception {
        Library functionLibrary = FuntionLibraryXMLParser.parseXML(libraryXMLPath);
        System.out.println(functionLibrary);
        return functionLibrary;
    }

    // Each library is installed as part of a transaction. Any failure in
    // installing a library rolls triggers uninstallation
    // of all installed components of the library but does not effect
    // installation of other libraries.
    private static Library installLibrary(Library library) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        MetadataManager.INSTANCE.lock(mdTxnCtx, LockMode.EXCLUSIVE);
        try {
            if (MetadataManager.INSTANCE.getDataverse(mdTxnCtx, library.getDataverse()) == null) {
                MetadataManager.INSTANCE.addDataverse(mdTxnCtx, new Dataverse(library.getDataverse(),
                        NonTaggedDataFormat.NON_TAGGED_DATA_FORMAT));
            }
            for (ILibraryElement element : library.getElements()) {
                switch (element.getType()) {

                    case FUNCTION:
                        FunctionElement fe = (FunctionElement) element;
                        Function function = new Function(library.getDataverse(),
                                library.getName() + "." + fe.getName(), fe.getArgumentList().size(),
                                fe.getArgumentList(), fe.getReturnType(), fe.getDefintion(),  library.getLanguage(),
                                fe.getFunctionType());
                        MetadataManager.INSTANCE.addFunction(mdTxnCtx, function);
                        break;

                    case ADAPTER:
                        AdapterElement ae = (AdapterElement) element;
                        AdapterIdentifier aid = new AdapterIdentifier(library.getDataverse(), library.getName() + "."
                                + ae.getName());
                        Adapter adapter = new Adapter(aid, ae.getClassname(), Adapter.AdapterType.EXTERNAL);
                        MetadataManager.INSTANCE.addAdapter(mdTxnCtx, adapter);
                        break;
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return library;
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        }

    }

    private static ClassLoader getLibraryClassLoader(String nodeId, String libraryDir, Library library)
            throws MalformedURLException {
        File deployDir = ExternalLibraryManager.getExternalLibraryDeployDir(nodeId);
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
}

class FuntionLibraryXMLParser {

    public static Library parseXML(File inputFile) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(inputFile);
        doc.getDocumentElement().normalize();
        return getLibrary(doc);
    }

    private static Library getLibrary(Document document) {
        Element rootEle = document.getDocumentElement();
        NodeList nodeList = rootEle.getChildNodes();
        String language = getStringValue((Element) nodeList, "Language");
        String dataverse = getStringValue((Element) nodeList, "Dataverse");
        String name = getStringValue((Element) nodeList, "Name");
        List<ILibraryElement> elements = getFunctions(document);
        elements.addAll(getAdapters(document));
        return new Library(dataverse, name, language, elements);
    }

    private static List<ILibraryElement> getAdapters(Document document) {
        List<ILibraryElement> adapters = new ArrayList<ILibraryElement>();
        Element rootEle = document.getDocumentElement();
        NodeList nodeList = rootEle.getElementsByTagName("Adapter");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            adapters.add(getAdapter((Element) node));
        }
        return adapters;
    }

    private static List<ILibraryElement> getFunctions(Document document) {
        Element rootEle = document.getDocumentElement();
        NodeList nodeList = rootEle.getElementsByTagName("Function");
        List<ILibraryElement> functionElement = new ArrayList<ILibraryElement>();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            functionElement.add(getFunction((Element) node));
        }
        return functionElement;
    }

    private static AdapterElement getAdapter(Element element) {
        String name = getStringValue(element, "Name");
        String classname = getStringValue(element, "Class");
        AdapterElement ae = new AdapterElement(name, classname);
        return ae;
    }

    private static FunctionElement getFunction(Element functionElement) {
        String type = functionElement.getAttribute("Type");
        String name = getStringValue(functionElement, "Name");
        String argumentProperty = getStringValue(functionElement, "ArgumentList");
        String returnType = getStringValue(functionElement, "ReturnType");
        String definition = getStringValue(functionElement, "Definition");
        String[] arguments = argumentProperty.split(",");
        List<String> argumentList = new ArrayList<String>();
        for (String argument : arguments) {
            argumentList.add(argument);
        }

        FunctionElement fe = new FunctionElement(type, name, argumentList, returnType, definition);
        return fe;
    }

    private static String getStringValue(Element element, String tagName) {
        String textValue = null;
        NodeList nl = element.getElementsByTagName(tagName);
        if (nl != null && nl.getLength() > 0) {
            Element el = (Element) nl.item(0);
            textValue = el.getFirstChild().getNodeValue();
        }
        return textValue;
    }

    public static void main(String args[]) {
        String dirPath = "/home/raman/research/workspaces/asterix-functions/misc/library";
        String libraryPath = dirPath + "/" + "math_functions.lib";
        File file = new File(libraryPath);
        try {
            parseXML(file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

class Library {

    private final String dataverse;
    private final String name;
    private final String language;
    private final List<ILibraryElement> elements;

    public Library(String dataverse, String name, String language, List<ILibraryElement> elements) {
        this.dataverse = dataverse;
        this.name = name;
        this.language = language;
        this.elements = elements;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder("");
        builder.append("Library");
        builder.append("\n");
        builder.append("Functions");
        builder.append("\n");
        for (ILibraryElement le : elements) {
            builder.append(le);
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

    public List<ILibraryElement> getElements() {
        return elements;
    }

}

interface ILibraryElement {

    public enum ElementType {
        FUNCTION,
        ADAPTER
    }

    public String getName();

    public ElementType getType();

}

class FunctionElement implements ILibraryElement {
    private final String name;
    private final ElementType type;
    private final String functionType;
    private final List<String> argumentList;
    private final String returnType;
    private final String defintion;

    public FunctionElement(String functionType, String name, List<String> argumentList, String returnType,
            String definition) {
        this.name = name;
        this.type = ILibraryElement.ElementType.FUNCTION;
        this.functionType = functionType;
        this.argumentList = argumentList;
        this.returnType = returnType;
        this.defintion = definition;
    }

    public ElementType getType() {
        return type;
    }

    public String getFunctionType() {
        return functionType;
    }

    public String getName() {
        return name;
    }

    public List<String> getArgumentList() {
        return argumentList;
    }

    public String getReturnType() {
        return returnType;
    }

    public String getDefintion() {
        return defintion;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(functionType);
        builder.append(" ");
        builder.append(returnType);
        builder.append(" ");
        builder.append(name);
        builder.append("(");
        for (String argument : argumentList) {
            builder.append(argument);
            builder.append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        builder.append("\n");
        builder.append(defintion);
        return new String(builder);
    }

}

class AdapterElement implements ILibraryElement {

    private final ElementType type;
    private final String name;
    private final String classname;

    public AdapterElement(String name, String classname) {
        this.name = name;
        this.type = ElementType.ADAPTER;
        this.classname = classname;
    }

    public ElementType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getClassname() {
        return classname;
    }

}
