package edu.uci.ics.asterix.api.http.servlet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class SpatialAggregationDemoAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";
    private String query;
    private boolean firstTime = true;
    private String dataFilePath;
    private String outputDirPath;
    private String asterixDirPath;

    private String generateQuery(HttpServletRequest request) {
        String fmt, formatted;
        if (firstTime) {
            fmt =   "drop dataverse twitter if exists;\n" +
            		"create dataverse twitter;\n" +
					"use dataverse twitter;\n\n" +
					
					"create type Tweet as closed {\n" +
					"	id: int32,\n" +
					"	tweetid: int64,\n" +
					"	loc: point,\n" +
					"	time: datetime,\n" +
					"	text: string\n" +
					"}\n" +
					"create nodegroup group1 if not exists on nc1, nc2;\n" +

					"create dataset TwitterData(Tweet)\n" +
					 " partitioned by key id on group1;\n" +

					"load dataset TwitterData " +
					"using \"edu.uci.ics.asterix.external.dataset.adapter.NCFileSystemAdapter\"" +
					"((\"path\"=\"nc1://" + dataFilePath + "\"),(\"format\"=\"adm\")) pre-sorted;\n";

            query = escapeQuotes(fmt);
            return fmt;
        }

        fmt =   "use dataverse twitter;\n\n" +
				
				"write output to nc1:\"%s/%s-%s.adm\";\n\n" +

                "for $t in dataset(\'TwitterData\')\n" +
                "let $keyword := \"%s\" \n" +
                "let $region := polygon(\"\n\t%s,%s \n\t%s,%s \n\t%s,%s \n\t%s,%s\")\n\n" +
                "where spatial-intersect($t.loc, $region) and\n" +
                "$t.time > datetime(\"%s\") and $t.time < datetime(\"%s\") and\n" +
                "contains($t.text, $keyword)\n\n" +
                "group by $c := spatial-cell($t.loc, create-point(24.5,-125.5), %s, %s) with $t\n\n" +
                "return { \"cell\": $c, \"count\": count($t) }";
        
     
        String fmtWithComments =
                "// A dataverse is the ASTERIX equivalent to a database in the relational world\n" +
        		"drop dataverse twitter if exists;\n" +
        		"create dataverse twitter;\n" +
				"use dataverse twitter;\n\n" +

                "// Defining a record called \"Tweet\" to describe a tweet.\n" +
                "create type Tweet as closed {\n" +
				"	id: int32,\n" +
				"	tweetid: int64,\n" +
				"	loc: point,\n" +
				"	time: datetime,\n" +
				"	text: string\n" +
				"}\n\n" +

                "// A few physical details (for now).\n" +
                "create nodegroup group1 if not exists on nc1, nc2;\n\n" +

                "// A dataset is the ASTERIX equivalent to a table.\n" +
                "create dataset TwitterData(Tweet)\n" +
				 " partitioned by key id on group1;\n" +
                
				"load dataset TwitterData " +
				"using \"edu.uci.ics.asterix.external.dataset.adapter.NCFileSystemAdapter\"" +
				"((\"path\"=\"nc1://PATH_TO_FILE/tweets.txt\"),(\"format\"=\"adm\")) pre-sorted;\n" +
                "write output to nc1:OUTPUT_FILE;\n\n" +

                "for $t in dataset(\'TwitterData\')\n" +
                "let $keyword := \"%s\" \n" +
                "let $region := polygon(\"\n\t%s,%s \n\t%s,%s \n\t%s,%s \n\t%s,%s\")\n\n" +
                "// Constrain tweets to bounding region, datetime window, and those containing the keyword\n" +
                "where spatial-intersect($t.loc, $region) and\n" +
                "$t.time > datetime(\"%s\") and $t.time < datetime(\"%s\") and\n" +
                "contains($t.text, $keyword)\n\n" +
                "// Use spatial-cell to determine the grid cell a tweet is in based on its location.\n" +
                "// Group tweets according to their containing grid cells.\n" +
                "group by $c := spatial-cell($t.loc, create-point(24.5,-125.5), %s, %s) with $t\n\n" +
                "return { \"cell\": $c, \"count\": count($t) }";

        Date d = new Date();
        SimpleDateFormat dFormat = new SimpleDateFormat("yyMMdd-HHmmss");

        String glat = request.getParameter("gridlat");
        glat = (glat.indexOf(".") == -1) ? glat+".0" : glat;
        String glng = request.getParameter("gridlng");
        glng = (glng.indexOf(".") == -1) ? glng+".0" : glng;

        formatted = String.format(fmt,
                outputDirPath, dFormat.format(d),
                request.getRemoteAddr(), request.getParameter("keyword"),
                request.getParameter("neLat"), request.getParameter("swLng"),   // sw
                request.getParameter("swLat"), request.getParameter("swLng"),   // nw
                request.getParameter("swLat"), request.getParameter("neLng"),   // ne
                request.getParameter("neLat"), request.getParameter("neLng"),   // se
                request.getParameter("startdt"), request.getParameter("enddt"),
                glat, glng);
        query = escapeQuotes(String.format(fmtWithComments, request.getParameter("keyword"),
                request.getParameter("neLat"), request.getParameter("swLng"),   // sw
                request.getParameter("swLat"), request.getParameter("swLng"),   // nw
                request.getParameter("swLat"), request.getParameter("neLng"),   // ne
                request.getParameter("neLat"), request.getParameter("neLng"),   // se
                request.getParameter("startdt"), request.getParameter("enddt"),
                glat, glng));

        return formatted;
    }

    private void writeResults(File localFile, PrintWriter out) throws IOException, JSONException {
        BufferedReader reader = new BufferedReader(new FileReader(localFile));
        int maxcount = -1;

        ArrayList<JSONObject> objArray = new ArrayList<JSONObject>(50);
        JSONObject jobj;
        String[] split1, split2;
        String cell;
        String inputLine = reader.readLine();

        while (inputLine != null) {
            inputLine = inputLine.replaceAll("\\Qrectangle(\\E|\\Q)\\E", "");
            jobj = new JSONObject(inputLine);
            cell = jobj.getString("cell");
            split1 = cell.split("\\Q | \\E");
            split2 = split1[0].split(",");
            jobj = jobj.put("x1", Double.parseDouble(split2[0]));
            jobj = jobj.put("y1", Double.parseDouble(split2[1]));
            split2 = split1[1].split(",");
            jobj = jobj.put("x2", Double.parseDouble(split2[0]));
            jobj = jobj.put("y2", Double.parseDouble(split2[1]));
            jobj.remove("cell");
            objArray.add(jobj);
            inputLine = reader.readLine();
            maxcount = (jobj.getInt("count") > maxcount) ? jobj.getInt("count") : maxcount;
        }

        String red;
        String green;
        for (int i = 0; i < objArray.size(); i++) {
            jobj = objArray.get(i);
            if ((double) jobj.getInt("count") / (double) maxcount < 0.5) {
                red = Integer.toHexString((int) (Math.ceil((double) jobj.getInt("count")
                        / ((double) maxcount / 2.0) * 255.0)));
                green = "ff";
            } else {
                red = "ff";
                green = Integer
                        .toHexString((int) (Math.ceil((double) (jobj.getInt("count") - maxcount)
                                * -1.0 / ((double) maxcount / 2.0) * 255.0)));
            }

            if (red.length() == 1) {
                red = "0" + red;
            }

            if (green.length() == 1) {
                green = "0" + green;
            }
            jobj.put("cellColor", "#" + red + green + "00");
        }
        jobj = new JSONObject("{ \"query\": \"" + query + "\"}");
        objArray.add(jobj);
        jobj = new JSONObject("{ \"maxcount\": " + maxcount + "}");
        objArray.add(jobj);

        JSONArray jArray = new JSONArray(objArray);
        out.write(jArray.toString());
    }

    private String escapeQuotes(String s) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '\"' || s.charAt(i) == '\n' || s.charAt(i) == '\t') {
                sb.append("\\");
            }
            sb.append(s.charAt(i));
        }

        return sb.toString();
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    	if(firstTime) {
    		dataFilePath = request.getParameter("data-file-path");
    		outputDirPath = request.getParameter("output-dir-path");
    		asterixDirPath = request.getParameter("asterix-dir-path");
    		runQuery(request, response);
    		firstTime = false;
    	} else {
    		runQuery(request, response);
    	}   
    }
    
    private void runQuery(HttpServletRequest request, HttpServletResponse response) throws IOException {
    	PrintWriter nullWriter = new PrintWriter("/dev/null");
        PrintWriter out = response.getWriter();
        String strIP = "localhost";
        response.setContentType("text/html");
        int port = 1098;
        String query = generateQuery(request);
        
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
                    dataverseName, q, parser.getVarCounter(), null, null, pc, nullWriter, DisplayFormat.HTML, null);
            JobSpecification spec = metadataAndSpec.second;
            AqlCompiledMetadataDeclarations metadata = metadataAndSpec.first;
            APIFramework.executeJobArray(hcc, new JobSpecification[] { spec }, null, DisplayFormat.HTML); 
            writeResults(metadata.getOutputFile().getLocalFile().getFile(), out);
        } catch (Exception e) {
            e.printStackTrace(out);
        }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    	if (firstTime) {
    		PrintWriter out = response.getWriter();
	        response.setContentType("text/html");
	        final String form = "<form method=\"post\">"
	                + "Tweets file absolute path: <input type = \"text\" name = \"data-file-path\" size=\"100\"  /><br/>"
	                + "Output directory absolute path: <input type = \"text\" name = \"output-dir-path\" size=\"100\" /><br/>"
	                + "ASTERIX directory absolute path: <input type = \"text\" name = \"asterix-dir-path\" size=\"100\" /><br/>"
	
	                + "<input type=\"submit\"/>" + "</center>" + "</form>";
	        out.println(form);
    	}
    	else {
	        OutputStream out = response.getOutputStream();
	        String path = request.getRequestURI();
	        File file;
	
	        response.setContentType("text/html");
	        if (path.equals("/") || path.equals("/index.html")) {
	        	file = new File(asterixDirPath + "/asterix-app/data/spatial-agg-demo/index.html");
	        } else if (path.equals("/lib.js")) {
	            response.setContentType("application/javascript"); 
	            file = new File(asterixDirPath + "/asterix-app/data/spatial-agg-demo/lib.js");
	        } else if (path.equals("/style.css")) {
	            response.setContentType("text/css");
	            file = new File(asterixDirPath + "/asterix-app/data/spatial-agg-demo/style.css");
	        } else if (path.equals("/asterix-characters.jpg")) {
	            response.setContentType("image/jpeg");
	            file = new File(asterixDirPath + "/asterix-app/data/spatial-agg-demo/asterix-characters.jpg");
	        } else if (path.equals("/asterix-logo.gif")) {
	            response.setContentType("image/gif");
	            file = new File(asterixDirPath + "/asterix-app/data/spatial-agg-demo/asterix-logo.gif");
	        } else if (path.equals("/spinner.gif")) {
	            response.setContentType("image/gif");
	            file = new File(asterixDirPath + "/asterix-app/data/spatial-agg-demo/spinner.gif");
	        } else if (path.equals("/jquery.spinner.js")) {
	            response.setContentType("application/javascript");
	            file = new File(asterixDirPath + "/asterix-app/data/spatial-agg-demo/jquery.spinner.js");
	        } else {
	            response.setStatus(javax.servlet.http.HttpServletResponse.SC_NOT_FOUND);
	            final String error404 = "<h1>404 Error: Can't find resource " +
	                path + " </h1>";
	            byte[] byteArray = error404.getBytes("UTF-8");
	            out.write(byteArray, 0, byteArray.length);
	            return;
	        }
	        response.setContentLength((int)file.length());
	
	        FileInputStream in = new FileInputStream(file);
	        byte[] buf = new byte[1024];
	        int count = 0;
	        while ((count = in.read(buf)) >= 0) {
	            out.write(buf, 0, count);
	        }
	        in.close();
	        out.close();
    	}
    }

    private String postDmlStatement(IHyracksClientConnection hcc, Query dummyQ, PrintWriter out, SessionConfig pc)
            throws Exception {
        String dataverseName = APIFramework.compileDdlStatements(hcc, dummyQ, out, pc, DisplayFormat.TEXT);
        Job[] dmlJobSpecs = APIFramework.compileDmlStatements(dataverseName, dummyQ, out, pc, DisplayFormat.HTML);
        APIFramework.executeJobArray(hcc, dmlJobSpecs, null, DisplayFormat.HTML);
        return dataverseName;
    }
}
