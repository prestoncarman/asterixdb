package edu.uci.ics.asterix.api.http.servlet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class QueryServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String query = request.getParameter("query");
        String strIP = request.getParameter("hyracks-ip");
        String strPort = request.getParameter("hyracks-port");
        int port = Integer.parseInt(strPort);
        PrintWriter out = response.getWriter();
        response.setContentType("text/html");
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        try {
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
                if (hcc == null) {
                    hcc = new HyracksConnection(strIP, port);
                    context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
                }
            }
            AQLParser parser = new AQLParser(new StringReader(query));
            Query q = (Query) parser.Statement();
            SessionConfig pc = new SessionConfig(port, true, false, false, false, false, false, false);
            pc.setGenerateJobSpec(true);

            MetadataManager.INSTANCE.init();
            String dataverseName = null;

            if (q != null) {
                dataverseName = postDmlStatement(hcc, q, out, pc);
            }

            if (q.isDummyQuery()) {
                return;
            }

            Pair<AqlCompiledMetadataDeclarations, JobSpecification> metadataAndSpec = APIFramework.compileQuery(
                    dataverseName, q, parser.getVarCounter(), null, null, pc, out, DisplayFormat.HTML, null);
            JobSpecification spec = metadataAndSpec.second;
            AqlCompiledMetadataDeclarations metadata = metadataAndSpec.first;
            APIFramework.executeJobArray(hcc, new JobSpecification[] { spec }, out, DisplayFormat.HTML);

            displayFile(metadata.getOutputFile().getLocalFile().getFile(), out);
        } catch (ParseException pe) {
            String message = pe.getMessage();
            message = message.replace("<", "&lt");
            message = message.replace(">", "&gt");
            out.println("SyntaxError:" + message);
            int pos = message.indexOf("line");
            if (pos > 0) {
                int columnPos = message.indexOf(",", pos + 1 + "line".length());
                int lineNo = Integer.parseInt(message.substring(pos + "line".length() + 1, columnPos));
                String line = query.split("\n")[lineNo - 1];
                out.println("==> " + line);
            }
        } catch (Exception e) {
            out.println(e.getMessage());
            e.printStackTrace(out);
        }
    }

    private String postDmlStatement(IHyracksClientConnection hcc, Query dummyQ, PrintWriter out, SessionConfig pc)
            throws Exception {

        String dataverseName = APIFramework.compileDdlStatements(hcc, dummyQ, out, pc, DisplayFormat.TEXT);
        Job[] dmlJobSpecs = APIFramework.compileDmlStatements(dataverseName, dummyQ, out, pc, DisplayFormat.HTML);

        APIFramework.executeJobArray(hcc, dmlJobSpecs, out, DisplayFormat.HTML);
        return dataverseName;
    }

    private void displayFile(File localFile, PrintWriter out) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(localFile));
        String inputLine = reader.readLine();
        while (inputLine != null) {
            out.println(inputLine);
            inputLine = reader.readLine();
        }
        reader.close();
    }
}
