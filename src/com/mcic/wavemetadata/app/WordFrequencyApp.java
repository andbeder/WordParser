package com.mcic.wavemetadata.app;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.mcic.ConfiguredApp;
import com.mcic.sfrest.SalesforceModel;
import com.mcic.sfrest.SalesforceREST;
import com.mcic.util.GZipBase64RecordSet;
import com.mcic.util.Progressive;
import com.mcic.util.RecordSet;
import com.mcic.util.RecordsetOld;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.util.json.ThreadCluster;
import com.mcic.wavemetadata.ui.ProgressPanel.ProgressPanelStep;

public class WordFrequencyApp extends ConfiguredApp {
	SalesforceREST agent;
	Vector<String> fields;
	public static String sourceDatasetId = null;
	public String caseId;
	private boolean writeCSV;
	private Progressive dialog;

	public static final String TARGET_DATASET_NAME = "Word_Frequency_File";
	public static final String TARGET_DATASET_LABEL = "Word Frequency (File)";
	public static final String SOURCE_DATASET_NAME = "Report_PSLP_with_Layers";

	public static void main(String[] args) {
		WordFrequencyApp app = new WordFrequencyApp();
		Vector<String> additionalArgs = main(args, app);
		Vector<String> fields = new Vector<String>();
		app.caseId = null;
		app.writeCSV = false;

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
			case "-c":
			case "-csv":
				app.writeCSV = true;
				break;
			default:
				break;
			}
			i++;
		}

		if (fields.size() == 0) {
			System.out.println("Use the following command line arguments:\n"
					+ "  -f -fields  <comma-separated list of field names to search>\n"
					+ "  -d -dir  <name of directory containing login parameters>\n" + "  -id <PSLP Case Id>"
					+ "  -sf <name of file containing login parameters>");
		} else {
			app.fields = fields;
			app.init();
		}
	}

	@Override
	public void init() {
		File propFile = (File) properties.get("sfConfig");
		// Collection<String> datasets = (Collection<String>)properties.get("datasets");
		SalesforceModel model = new SalesforceModel(propFile);
		agent = new SalesforceREST(model);
		readFields();
		dialog = new Progressive();
		nextStep("Upload completed, you may close this window", true);
		setClose(true);
	}


	private String getDatasetId() {
		if (sourceDatasetId == null) {
			String url = "/services/data/v58.0/wave/datasets?q=Layers";
			int res = agent.get(url, null);
			JSONNode data = agent.getResponse();
			if (res == SalesforceREST.SUCCESS) {
				for (JSONNode n : data.get("datasets").values()) {
					String name = n.get("name").asString();
					if (name.equals(SOURCE_DATASET_NAME)) {
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

	public class Keyword {
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

	public void readFields() {
		

		nextStep("Querying CRM Analytics dataset", true);

		boolean readSalesforce = false;
		if (readSalesforce) {
			Map<String, Keyword> freq = new TreeMap<String, Keyword>();

			String nextUrl = "/services/data/v58.0/query?q=SELECT+Id,"
					+ fields.stream().collect(Collectors.joining(",")) + "+FROM+MCIC_Patient_Safety_Case__c";
			while (nextUrl != null) {
				agent.get(nextUrl, null);
				JSONNode data = agent.getResponse();
				for (JSONNode record : data.get("records").values()) {
					String caseId = record.get("Id").asString();
					for (String field : fields) {
						String rec = record.get(field).asString();
						// Remove all XML markup
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

			ThreadCluster cluster = new ThreadCluster(5);
			Map<String, Keyword> freq = new ConcurrentHashMap<String, Keyword>();

			for (int year = 2030; year >= 2000; year -= 1) {
				String saql = "q = load \\\"" + getDatasetId() + "\\\";";
				if (year > 2000) {
					saql += "q = filter q by 'Date_of_Loss__c_YEAR' == \\\"" + year + "\\\";";
				} else {
					saql += "q = filter q by 'Date_of_Loss__c_YEAR' <= \\\"" + year + "\\\";";
				}
				if (caseId != null) {
					saql += " q = filter q by 'Patient.Patient_Safety_Case__c' == \\\"" + caseId + "\\\";";
				}
				saql += "q = filter q by 'Baby_Flag' == \\\"No\\\";";
				saql += "q = group q by Id;" + "q = foreach q generate Id, first(Claim_Made_Date) as Claim_Made_Date, "
						+ fields.stream().map((field) -> "first('" + field + "') as '" + field + "'")
								.collect(Collectors.joining(", "))
						+ ";" + "q = order q by Claim_Made_Date desc;";
				JSONObject post = new JSONObject();
				post.addString("query", saql);

				/************************************************************
				 * Kick off separate theads to pull the CRMA data
				 */

				final String policyYear = year + "";
				final Progressive thisProg = this;

				cluster.dispatch(new Runnable() {

					public void run() {
						//System.out.println("This is the policy year for the thread: " + policyYear);

						JSONNode data = null;
						ProgressPanelStep step = thisProg.nextStep("Reading policy year " + policyYear);
						while (data == null) {
							int r = agent.postJSON("/services/data/v60.0/wave/query", post);
							if (r == SalesforceREST.SUCCESS) {
								data = agent.getResponse();
							} else if (data.getType() == JSONNode.Type.ARRAY ){
								System.out.println("Query error: " + data.toString());
							}
						}
						
						if (data.getType() == JSONNode.Type.OBJECT) {

							for (JSONNode record : data.get("results").get("records").values()) {
								String caseId = record.get("Id").asString();
								for (String field : fields) {
									if (record.get(field) != null) {
										String rec = record.get(field).asString();
	
										// Remove all XML markup
										String sentence = rec.replaceAll("(<(.*?)>)", "").toLowerCase();
										// Replace all newlines with '.' characters
										sentence = sentence.replaceAll("\\\\n", "\\.");
	
										if (sentence != null && !sentence.equals("null")) {
											sentence = " " + sentence + " ";
											int start = 0;
											// Identify groups of three words
											Matcher matcher = Pattern
													.compile("[\\W][\\w%:]+[\\W][\\w%:]+[\\W][\\w%:]+[\\W]")
													.matcher(sentence);
											while (matcher.find(start)) {
												int s = matcher.start();
												int e = matcher.end();
												String phrase = sentence.substring(s + 1, e - 1);
												countWord(freq, phrase, field, "Phrase", caseId);
												start = s + 1;
											}
											// Identify groups of two words
											start = 0;
											matcher = Pattern.compile("[\\W][\\w%:]+[\\W][\\w%:]+[\\W]").matcher(sentence);
											while (matcher.find(start)) {
												int s = matcher.start();
												int e = matcher.end();
												String phrase = sentence.substring(s + 1, e - 1);
												countWord(freq, phrase, field, "Phrase", caseId);
												start = s + 1;
											}
	
											start = 0;
											// Identify single words
											matcher = Pattern.compile("[\\W][\\w%:]+[\\W]").matcher(sentence);
											while (matcher.find(start)) {
												int s = matcher.start();
												int e = matcher.end();
												String phrase = sentence.substring(s + 1, e - 1);
												countWord(freq, phrase, field, "Word", caseId);
												start = s + 1;
											}
										}
									}
								}
							}
						}  else {
							System.out.println("Dataset Query Error");
						}
						step.complete();
					}
				});
				// End of Runnable

			}
			cluster.join();

			nextStep("Uploading Word Frequency (File) dataset to CRM Analytics", true);
			writeDatasets(freq, "Overwrite");
		}
	}

	public void writeDatasets(Map<String, Keyword> freq, String operation) {
		ProgressPanelStep step = nextStep("Compressing and encoding query data");
		RecordSet wordFreq = new GZipBase64RecordSet();
		int total = 0;
		for (Keyword key : freq.values()) {
			total += key.cases.size();
		}
		System.out.println("There are " + total + " total case IDs");
		
		int i = 0;
		for (String key : freq.keySet()) {
			Keyword k = freq.get(key);
			String word = k.word;
			for (String id : k.cases) {
				if (!word.equals("")) {
					if (i++ % 100000 == 0) {
						double pct = 100 * (double)i / (double)total;
						System.out.println("Processing case ID # " + i + " which is " + pct + "%");
						step.setText("Compressing and encoding query data, " + String.format("%.1f", pct) + "% complete");
					}
					
					wordFreq.add("Field", k.field);
					wordFreq.add("Type", k.type);
					wordFreq.add("Word", word);
					wordFreq.add("CaseId", id);
					wordFreq.next();
				}
			}
		}
		step.complete();
		if (writeCSV) {
			try {
				FileWriter out = new FileWriter(new File("C:\\Users\\abeder\\Downloads\\dataset.csv"));
				String encoded = wordFreq.toBase64();
				byte[] zipped = Base64.getDecoder().decode(encoded.getBytes());
				ByteArrayInputStream decompressed = new ByteArrayInputStream(zipped);
				GZIPInputStream gzipIn = new GZIPInputStream(decompressed);
				ByteArrayOutputStream decoded = new ByteArrayOutputStream();

				byte[] buffer = new byte[1024];
				int bytesRead;
				while ((bytesRead = gzipIn.read(buffer)) != -1) {
					decoded.write(buffer, 0, bytesRead);
				}
				String csv = new String(decoded.toByteArray(), StandardCharsets.UTF_8);
				out.write(csv);
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			// String operation = isFirst ? "Overwrite" : "Append";
			// isFirst = false;
			String metadata = "{\"fileFormat\":{\"charsetName\":\"UTF-8\",\"fieldsDelimitedBy\":\",\",\"linesTerminatedBy\":\"\\r\\n\"},\"objects\":[{\"connector\":\"CSV\",\"fullyQualifiedName\":\"download_6_csv\",\"label\":\"download (6).csv\",\"name\":\"download_6_csv\",\"fields\":[{\"fullyQualifiedName\":\"Field\",\"name\":\"Field\",\"type\":\"Text\",\"label\":\"Field\"},{\"fullyQualifiedName\":\"Type\",\"name\":\"Type\",\"type\":\"Text\",\"label\":\"Type\"},{\"fullyQualifiedName\":\"Word\",\"name\":\"Word\",\"type\":\"Text\",\"label\":\"Word\"},{\"fullyQualifiedName\":\"CaseId\",\"name\":\"CaseId\",\"type\":\"Text\",\"label\":\"CaseId\"}]}]}";
			agent.writeDataset(TARGET_DATASET_NAME, TARGET_DATASET_LABEL, "RDI_Development", wordFreq, operation, metadata);
			// System.out.println(out.toString());
		}
	}

	public void countWord(Map<String, Keyword> freq, String word, String field, String type, String caseId) {
		if (word != null && !word.equals("")) {
			String key = field + "|" + word;
			Keyword k = freq.get(key);
			if (k == null) {
				k = new Keyword(word, field, type);
				freq.put(key, k);
			}
			k.cases.add(caseId);
		}
	}
}
