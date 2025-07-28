package com.mcic.wavemetadata.app;

import java.awt.Dimension;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.swing.JDialog;

import com.mcic.ConfiguredApp;
import com.mcic.sfrest.SalesforceREST;
import com.mcic.sfrest.SalesforceAgentOld;
import com.mcic.sfrest.SalesforceModel;
import com.mcic.util.GZipBase64RecordSet;
import com.mcic.util.RecordsetOld;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.util.json.ThreadCluster;
import com.mcic.wavemetadata.ui.ProgressPanel;
import com.mcic.wavemetadata.ui.ProgressPanel.ProgressPanelStep;

public class WordFrequencyApp2 extends ConfiguredApp {
	SalesforceREST agent;
	Vector<String> fields;
	Map<String, Keyword> freq;
	public static String datasetName;
	public static String datasetId = null;
	ProgressPanel progress;
	public String caseId;

	public static void main(String[] args) {
		WordFrequencyApp2 app = new WordFrequencyApp2();
		Vector<String> additionalArgs = main(args, app);
		Vector<String> fields =  new Vector<String>();
		datasetName = "Report_PSLP_with_Layers";
		app.caseId = null;

		int i = 0;
		while (i < additionalArgs.size()) {
			String cmd = additionalArgs.elementAt(i);
			switch (cmd.toLowerCase()) {
			case "-fields":
			case "-f":
				String ds = args[++i];
				for (String field : ds.split(",")) {
					fields.add(field);
				}
				break;
			case "-id":
				app.caseId = additionalArgs.elementAt(++i);
				break;
			default:
				break;
			}
			i++;
		}
		
		if (fields.size() == 0) {
			System.out.println("Use the following command line arguments:\n"
					+ "  -f -fields  <comma-separated list of field names to search>\n"
					+ "  -d -dir  <name of directory containing login parameters>\n"
					+ "  -id <PSLP Case Id>"
					+ "  -sf <name of file containing login parameters>");
		} else {
			app.fields = fields;
			app.init();
		}
	}
	
	@Override
	public void init() {
		File propFile = (File)properties.get("sfConfig"); 
		//Collection<String> datasets = (Collection<String>)properties.get("datasets");
		SalesforceModel model = new SalesforceModel(propFile);
		agent = new SalesforceREST(model);
		progress = new ProgressPanel(19);
		JDialog dialog = new JDialog();
		dialog.setBounds(100, 100, 450, 300);
		dialog.add(progress);
		dialog.setVisible(true);
		Map<String, Keyword> records = readFields();
		
		
		//progress.nextStep("Processing Upload");
		progress.setClose(true);
		//wordFreq.close();
	}
	
	private String getDatasetId() {
		if (datasetId == null) {
			String url = "/services/data/v58.0/wave/datasets?q=Layers";
			JSONNode data = agent.get(url, null);
			for (JSONNode n : data.get("datasets").values()) {
				String name = n.get("name").asString();
				if (name.equals(datasetName)) {
					datasetId = n.get("id").asString() + "/" + n.get("currentVersionId").asString();
				}
			}
		}
		return datasetId;
	}
	
	public class Keyword{
		public String word;
		public String field;
		public String type;
		public Set<String> cases;
		public Keyword(String word, String field, String type) {
			this.word = word;
			this.field = field;
			this.type = type;
			cases = new TreeSet<String>();
		}
		@Override
		public String toString() {
			return field + ":" + word;
		}
	}

	public Map<String, Keyword> readFields() {
		freq = new TreeMap<String, Keyword>();
		
		boolean readSalesforce = false;
		if (readSalesforce) {
			String nextUrl = "/services/data/v58.0/query?q=SELECT+Id," + fields.stream().collect(Collectors.joining(",")) + "+FROM+MCIC_Patient_Safety_Case__c";
			while (nextUrl != null) {
				JSONNode data = agent.get(nextUrl, null);
				for (JSONNode record : data.get("records").values()) {
					String caseId = record.get("Id").asString();
					for (String field : fields) {
						String rec = record.get(field).asString();
						//  Remove all XML markup
						if (rec != null) {
							String res = rec;
							String[] str = res.split("\\.");
							for (String sentence : str) {
								
								sentence = sentence.replaceAll("[^\\sa-zA-Z0-9]", " ").toLowerCase();
								sentence = sentence.replaceAll("  ", " ");
								for (String word : sentence.split(" ")) {
									Keyword k = freq.get(word);
									if (k == null) {
										k = new Keyword(word, field, "Word");
										freq.put(word, k);
									}
									k.cases.add(caseId);
								}
							}
						}
					}
				}
				nextUrl = data.get("nextRecordsUrl") == null ? null : data.get("nextRecordsUrl").asString();
				
			}
		} else {
		
			ConcurrentHashMap<Integer, JSONNode> records = new ConcurrentHashMap<Integer, JSONNode>();
			ThreadCluster c = new ThreadCluster(4);
			for (int year = 2024;year >= 2000;year -= 4) {
				ProgressPanelStep step = progress.nextStep("Reading policy year " + year + "...");
				String saql = "q = load \\\"" + getDatasetId() + "\\\";";
				if (year > 2000) {
					saql += "q = filter q by 'Date_of_Loss__c_YEAR' == \\\"" + year + "\\\";";
				} else {
					saql += "q = filter q by 'Date_of_Loss__c_YEAR' <= \\\"" + year + "\\\";";
				}
				if (caseId != null) {
					saql += " q = filter q by 'Patient.Patient_Safety_Case__c' == \\\"" + caseId + "\\\";";
				}
				saql +=  "q = group q by Id;"
						+ "q = foreach q generate Id, first(Claim_Made_Date) as Claim_Made_Date, " + 
						fields.stream()
						  .map((field) -> "first('" + field + "') as '" + field + "'")
						  .collect(Collectors.joining(", "))
						+ ";"
						+ "q = order q by Claim_Made_Date desc;";
				JSONObject post = new JSONObject();
				post.addString("query", saql);
				
				c.dispatch(new Runnable() {
					public void run() {
						JSONNode data = agent.postJSON("/services/data/v60.0/wave/query", post);
						
						for (JSONNode record : data.get("results").get("records").values()) {
							String caseId = record.get("Id").asString();
							for (String field : fields) {
								if (record.get(field) != null) {
									String rec = record.get(field).asString();
									
									//  Remove all XML markup
									String res = rec.replaceAll("<(.*?)>", "").toLowerCase();
									//  Replace all newlines with '.' characters
									res = res.replaceAll("\\\\n", "\\.");
									
									String[] str = res.split("\\.");
									for (String sentence : str) {
										if (sentence != null && !sentence.equals("null")) {
											sentence = " " + sentence + " ";
        int start = 0;
        Matcher matcher = Pattern.compile("[ \n]\w+[ \n]").matcher(sentence);
        while (matcher.find(start)) {
            int s = matcher.start();
            int e = matcher.end();
            String phrase = sentence.substring(s + 1, e - 1);
            countWord(phrase, field, "Word", caseId);
            start = s + 1;
        }
											
										}
									}
								}
							}
						}
					}
				});
				step.complete();
			}
			c.join();
			progress.nextStep("Processing dataset file...");
			String operation = year == 2024 ? "Overwrite" : "Append";
			writeDatasets(operation);
		}
		//System.out.println(data.toString());
		return freq;
	}
	
	public void writeDatasets(String operation) {
		RecordsetOld wordFreq = new RecordsetOld();
		for (String key : freq.keySet()) {
			Keyword k = freq.get(key);
			String word = k.word;
			for (String id : k.cases) {
				if (!word.equals("")) {
					wordFreq.add("Word", word);
					wordFreq.add("Type", k.type);
					wordFreq.add("Field", k.field);
					wordFreq.add("CaseId", id);
					wordFreq.next();
				}
			}
		}
		agent.setPanel(progress);
		boolean writeCSV = false;
		if (writeCSV) {
			try {
				FileWriter out = new FileWriter(new File("C:\\Users\\abeder\\Downloads\\dataset.csv"));
				out.write(wordFreq.toString());
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			//String operation = isFirst ? "Overwrite" : "Append";
			//isFirst = false;
			String metadata = "{\"fileFormat\":{\"charsetName\":\"UTF-8\",\"fieldsDelimitedBy\":\",\",\"linesTerminatedBy\":\"\\r\\n\"},\"objects\":[{\"connector\":\"CSV\",\"fullyQualifiedName\":\"download_6_csv\",\"label\":\"download (6).csv\",\"name\":\"download_6_csv\",\"fields\":[{\"fullyQualifiedName\":\"Field\",\"name\":\"Field\",\"type\":\"Text\",\"label\":\"Field\"},{\"fullyQualifiedName\":\"Type\",\"name\":\"Type\",\"type\":\"Text\",\"label\":\"Type\"},{\"fullyQualifiedName\":\"Word\",\"name\":\"Word\",\"type\":\"Text\",\"label\":\"Word\"},{\"fullyQualifiedName\":\"CaseId\",\"name\":\"CaseId\",\"type\":\"Text\",\"label\":\"CaseId\"}]}]}";
			agent.writeDataset("Word_Frequency_File", "Word Frequency (File)", "RDI_Development", wordFreq, operation, metadata);
			//System.out.println(out.toString());
		}
	}
	
	public void countWord(String word, String field, String type, String caseId) {
		String key = field + "|" + word;
		Keyword k = freq.get(key);
		if (k == null) {
			k = new Keyword(word, field, type);
			freq.put(key, k);
		}
		k.cases.add(caseId);
	}
}
