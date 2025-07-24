package com.mcic.wavemetadata.tool;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.mcic.ConfiguredApp;
import com.mcic.sfrest.SalesforceModel;
import com.mcic.sfrest.SalesforceREST;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.util.json.ThreadCluster;
import com.mcic.wavemetadata.app.WordFrequencyApp;

public class WordCorrelation extends ConfiguredApp {
	boolean writeCSV;
	SalesforceREST agent;
	String sourceDatasetId;

	public static void main(String[] args) {
		WordCorrelation app = new WordCorrelation();
		Vector<String> additionalArgs = main(args, app);
		Vector<String> fields = new Vector<String>();
		app.sourceDatasetId = null;

		int i = 0;
		while (i < additionalArgs.size()) {
			String cmd = additionalArgs.elementAt(i);
			switch (cmd.toLowerCase()) {
			case "-c":
			case "-csv":
				app.writeCSV = true;
				break;
			default:
				break;
			}
			i++;
		}

		app.init();
	}

	@Override
	public void init() {
		File propFile = (File) properties.get("sfConfig");
		SalesforceModel model = new SalesforceModel(propFile);
		agent = new SalesforceREST(model);
		Map<String, Double> factors = new ConcurrentHashMap<String, Double>(); 
		
		String saql = "q = load \\\"" + getDatasetId() + "\\\";\r\n";
		saql += "q = filter q by 'Dataset' == \\\"Word Frequency\\\";\r\n"
				+ "q = group q by xVar;\r\n"
				+ "q = foreach q generate xVar;";
		
		JSONObject post = new JSONObject();
		post.addString("query", saql);
		int r = agent.postJSON("/services/data/v60.0/wave/query", post);
		JSONNode data = agent.getResponse();
		for (JSONNode record : data.get("results").get("records").values()) {
			String word = record.get("xVar").asString();
			factors.put(word, 0.0);
		}
		
		//  Create queue of 100 words at a time
		Vector<String> wordStack = new Vector<String>();
		int i = 0;
		String inList = "";
		for (String word : factors.keySet()) {
			if (!inList.equals("")) {
				inList += ", ";
			}
			inList += "\\\"" + word + "\\\"";
			if (i++ == 100) {
				i = 0;
				wordStack.add(inList);
				inList = "";
			}
		}
		
		//  Run a set of threads to pull responses;
		ThreadCluster cluster = new ThreadCluster(5);
		i = 0;
		for (String list : wordStack) {
			if (i++ < 10) {
				cluster.dispatch(new Runnable() {
					public void run() {
						String saql = "q = load \\\"" + getDatasetId() + "\\\";";
						saql += "q = filter q by 'xVar' in [" + list + "];\r\n"
								+ "q = group q by (Record_Id, xVar);\r\n"
								+ "q = foreach q generate xVar,  max(x) as x, max(z) as z, (max(x) - avg(max(x)) over ([..] partition by xVar)) * (max(z) - avg(max(z)) over ([..] partition by xVar)) / (count() over ([..] partition by xVar) - 1) as xz;\r\n"
								+ "q = group q by xVar;\r\n"
								+ "q = foreach q generate xVar, sum(xz) / (stddevp(x) * stddevp(z)) as rxz;\r\n"
								+ "q = filter q by rxz is not null;\r\n"
								+ "q = order q by rxz desc;";
						
						JSONObject post = new JSONObject();
						post.addString("query", saql);
						int r = agent.postJSON("/services/data/v60.0/wave/query", post);
						JSONNode data = agent.getResponse();
						for (JSONNode record : data.get("results").get("records").values()) {
							String word = record.get("xVar").asString();
							double rxz = record.get("rxz").asDouble();
							factors.put(word, rxz);
						}
						System.out.println("Frequency analysis complete for " + list.substring(0, 20) + "...");
					}
				});
			}
		}
		cluster.join();

		try {
			FileWriter out = new FileWriter(new File("rxz.csv"));
			out.write("xVar,rzx\r\n");
			for (Entry<String, Double> es : factors.entrySet()) {
				out.write(es.getKey() + "," + es.getValue().toString() + "\r\n");
			}
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
	}


	private String getDatasetId() {
		if (sourceDatasetId == null) {
			String url = "/services/data/v58.0/wave/datasets?q=Regression";
			int res = agent.get(url, null);
			JSONNode data = agent.getResponse();
			if (res == SalesforceREST.SUCCESS) {
				for (JSONNode n : data.get("datasets").values()) {
					String name = n.get("name").asString();
					if (name.equals("Regression_Analysis")) {
						sourceDatasetId = n.get("id").asString() + "/" + n.get("currentVersionId").asString();
					}
				}
			} else {
				System.out.println(data.toString());
				System.exit(1);
			}
		}
		return sourceDatasetId;
	}

	
}
