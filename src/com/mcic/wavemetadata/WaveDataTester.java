package com.mcic.wavemetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.swing.JFileChooser;
import javax.swing.filechooser.FileFilter;

import com.mcic.sfrest.SalesforceAgentOld;
import com.mcic.sfrest.SalesforceModel;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.Pair;
import com.mcic.util.json.ThreadCluster;
import com.mcic.wavemetadata.tool.WaveMetadata;
import com.mcic.wavemetadata.tool.WaveMetadata.BaseDashboard;
import com.mcic.wavemetadata.tool.WaveMetadata.BaseDataset;
import com.mcic.wavemetadata.tool.WaveMetadata.Dashboard;
import com.mcic.wavemetadata.tool.WaveMetadata.Dataset;
import com.mcic.wavemetadata.tool.WaveMetadata.Field;
import com.mcic.wavemetadata.tool.WaveMetadata.Type;

public class WaveDataTester {
	SalesforceAgentOld prodAgent;
	SalesforceAgentOld devAgent;
	WaveMetadata prod;
	WaveMetadata dev;
	Map<String, Set<String>> ignores;
	Map<String, Set<String>> filters;
//	Map<String, BaseDataset> prodDatasets;
//	Map<String, BaseDataset> devDatasets;
//	Map<String, BaseDashboard> prodDashboards;
	
	private static void printHelp() {
		String out = "Usage: java -jar WaveDataTester.jar  "
				+ "  -?/help displayes this message"
				+ "  -dev <file.properties> PROPERTIES file containing Development salesforce configuration"
				+ "  -prod <file.properties> PROPERTIES file containing Production salesforce configuration"
				+ "  -ds/datasets <dataset API names, comma delimited> lists the dataset API name you want to test."
				+ "          No value tests all datasets referenced by Released & In Dev dashboards in production"
				+ "  -d/dir <directory name> sets default current directory"
				+ "  -i/ignore <filename> file containing list of <datset>,<fieldName> to ignore"
				+ "  -f/filter <filename> file containing list SAQL of filters to apply in 'DATASET,q = filter q...;' format"
				+ "  -dis/disposition <disposition> comma-delimited list of dashboard dispositions to include, restricts"
				+ "          analysis to only fields available on these dashboards"
				+ "  -r/recordcount check count of rows in addition to measures";
	}

	public static void main(String[] args) {
		File devFile = null;
		File prodFile = null;
		File ignore = null;
		File filterFile = null;
		Vector<String> datasets = new Vector<String>();
		List<String> dispositions = new LinkedList<String>();
		String ds = "";
		String curDir = "";
		boolean recordCounts = false;
		int i = 0;
		//boolean needHelp = false;
		
		if (args.length == 0) {
			printHelp();
			return;
		}
		
		while (i < args.length) {
			String cmd = args[i].toLowerCase();
			switch (cmd) {
			case "-dis":
			case "-disposition":
				dispositions = Arrays.stream(args[++i].split(",")).collect(Collectors.toList());
				break;
			case "-dev": 
				devFile = new File(curDir + args[++i]);
				break;
			case "-prod": 
				prodFile = new File(curDir + args[++i]);
				break;
			case "-datasets":
			case "-ds":
				ds = args[++i];
				for (String dataset : ds.split(",")) {
					datasets.add(dataset);
				}
				break;
			case "-dir":
			case "-d":
				curDir = args[++i];
				char c = curDir.charAt(curDir.length() - 1);
				if (c != '/' && c != '\\') {
					curDir += "/";
				}
				break;
			case "-i":
			case "-ignore":
				ignore = new File(args[++i]);
				break;
			case "-filter":
			case "-f":
				filterFile = new File(args[++i]);
				break;
			case "-?":
			case "-help":
				printHelp();
				return;
			case "-recordcount":
			case "-r":
				recordCounts = true;
			}
			i++;
		}
		
		if (curDir.equals("")) {
			curDir = "C:\\Users\\abeder\\eclipse-workspace\\MCIC_RDI";
		}

		JFileChooser fc = new JFileChooser();
		fc.setCurrentDirectory(new File(curDir));
		fc.setFileFilter(new FileFilter() { 
			public boolean accept(File fileName) {
				if (fileName.isDirectory())
					return true;
				return fileName.getName().endsWith(".properties");
			}
			public String getDescription() {
				return null;
			}});
		prodFile = (prodFile == null) ? WaveMetadata.chooseProps("Production", fc, null) : prodFile;
		devFile = (devFile == null) ? WaveMetadata.chooseProps("Development", fc, null) : devFile;
		
		WaveDataTester a = new WaveDataTester(prodFile, devFile, ignore, filterFile);
		datasets = (datasets == null) ? new Vector<String>() : datasets;
		a.executeTests(datasets, dispositions, recordCounts);
//		try {
//			FileWriter f = new FileWriter("results.txt");
//			f.write(out);
//			f.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}
	
	public WaveDataTester(File prod, File dev, File ignore, File filterFile) {
		this.prodAgent = new SalesforceAgentOld(new SalesforceModel(prod));
		this.devAgent = new SalesforceAgentOld(new SalesforceModel(dev));
		this.prod = new WaveMetadata(prodAgent);
		this.dev = new WaveMetadata(devAgent);
		filters = new TreeMap<String, Set<String>>();
		ignores = new TreeMap<String, Set<String>>();
		if (ignore != null) {
			try {
				BufferedReader in = new BufferedReader(new FileReader(ignore));
				while (in.ready()) {
					String line = in.readLine();
					String[] pair = line.split(",");
					Set<String> list = ignores.get(pair[0]);
					if (list == null) {
						list = new TreeSet<String>();
						ignores.put(pair[0], list);
					}
					list.add(pair[1]);
				}
				in.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (filterFile != null) {
			try {
				BufferedReader in = new BufferedReader(new FileReader(filterFile));
				while (in.ready()) {
					String line = in.readLine();
					String[] pair = line.split(",");
					Set<String> list = filters.get(pair[0]);
					if (list == null) {
						list = new TreeSet<String>();
						filters.put(pair[0], list);
					}
					list.add(pair[1]);
				}
				in.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void executeTests(final Collection<String> chosenDatasets, Collection<String> dispositions, boolean recordCounts) {
		dispositions = (dispositions == null) ? new LinkedList<String>() : dispositions;
		
		/*********************************************************************************
		 *   First: Load datasets and dashboards
		 */
		
		BaseDataset dashDispDs = prod.getBaseDatasets().get("RDI_Inventory");
		String dispList = dispositions.stream().map(disp -> "\"" + disp + "\"").collect(Collectors.joining(", "));
		String saql = "q = load \"" + dashDispDs.getSAQL() + "\";\r\n"
				+ "q = filter q by 'Asset_Type' == \"Dashboard\";\r\n";
		saql += (dispositions.size() == 0) ? "" : "q = filter q by 'Asset_Disposition' in [" + dispList +  "];\r\n";
		saql += "q = group q by (Asset_Name, Dataset_Name);\r\n"
			  + "q = foreach q generate Asset_Name, Dataset_Name, count(q) as 'A';";
		JSONNode results = prod.executeQuery(saql);
		Set<String> datasetNames = new TreeSet<String>();
		Set<String> dashboardNames = new TreeSet<String>();
		
		for (JSONNode record : results.get("results").get("records").values()) {
			datasetNames.add(record.get("Dataset_Name").asString());
			dashboardNames.add(record.get("Asset_Name").asString());
		}		
		datasetNames = datasetNames.stream().filter(name -> chosenDatasets.size() == 0 || chosenDatasets.contains(name)).collect(Collectors.toSet());
		
		prod.loadDatasets(datasetNames);
		prod.loadDashboards(dashboardNames);

		
		/*********************************************************************************
		 *   Second: Queue up all fields to be tested
		 *   
		 */
		
		
		Set<Field> fields = new TreeSet<Field>();
		for (Dashboard d : prod.getDashboards().values()) {
			fields.addAll(d.fields);
		}
		Set<Dataset> datasets = fields.stream().map(field -> field.parent).collect(Collectors.toSet());
		

		/********************************************************************
		 * Third: Develop cardinality of all the fields;
		 * 
		 */

		
		Map<Dataset, Map<Field, Integer>> cardinalities = new ConcurrentHashMap<Dataset, Map<Field, Integer>>();
		Map<Dataset, Set<Field>> fieldDecks = new ConcurrentHashMap<Dataset, Set<Field>>();
		ThreadCluster cluster = new ThreadCluster(WaveMetadata.CONCURRENCY);
		for (Dataset dataset : datasets) {
			Set<String> ignore = (ignores.get(dataset.name) == null) ? new TreeSet<String>() : ignores.get(dataset.name);
			Set<Field> fieldList = fields.stream().filter(field -> field.parent.compareTo(dataset) == 0).collect(Collectors.toSet());
			Map<Field, Integer> cards = new ConcurrentHashMap<Field, Integer>();
			cardinalities.put(dataset, cards);
			saql = "q = load \"" + dataset.getSAQL() + "\";\r\n"
					+ "q = group q by all;\r\n"
					+ "q = foreach q generate ";
			
			saql += fieldList.stream().map(f -> "unique('" +  f.name + "') as '" + f.name + "_count'").collect(Collectors.joining(","));			
			saql += ", count() as c;";
			final String theSaql = saql;
			Set<Field> fieldDeck = new TreeSet<Field>();
			fieldDecks.put(dataset, fieldDeck);
			if (fieldList.size() > 0) {
				cluster.dispatch(new Runnable(){
					public void run() {
						JSONNode r = prod.executeQuery(theSaql);
						JSONNode n = r.get("results").get("records").elementAt(0);
						for (Field field : fieldList) {
							if (field.type == Type.Dimension) {
								int c = n.get(field.name + "_count").asInt();
								cards.put(field, c);
								if (!ignore.contains(field.name)) {
									fieldDeck.add(field);
								}
							}
						}
					}
				});
			}
		}
		cluster.join();
		

		/********************************************************************
		 *  Fourth: Develop test scripts
		 * 
		 */
		

		Map<Dataset, LinkedList<Set<Field>>> testScripts = new TreeMap<Dataset, LinkedList<Set<Field>>>();
		for (Dataset d : datasets) {
			LinkedList<Set<Field>> scripts = new LinkedList<Set<Field>>();
			Set<String> ignore = (ignores.get(d.name) == null) ? new TreeSet<String>() : ignores.get(d.name);
			Set<Field> fieldList = fields.stream()
					.filter(field -> field.parent.compareTo(d) == 0 && !ignore.contains(field.name))
					.collect(Collectors.toSet());
			Set<Field> dimensions = fieldList.stream()
					.filter(field -> field.type == Type.Dimension)
					.collect(Collectors.toSet());
			
			for (Field field : dimensions) {
				Set<Field> inScript = new TreeSet<Field>();
				inScript.add(field);
				int car = cardinalities.get(d).get(field);
				List<Field> deck = fieldDecks.get(d).stream().collect(Collectors.toList());
				Collections.shuffle(deck);
				if (field.name.contains("Resolution_Age_Years") || field.name.contains("ToDate")) {
					System.err.println("err");
				}
				while (deck.size() > 0) {
					Field next = deck.remove(0);
					if (next != field) {
						int c = cardinalities.get(d).get(next);
						if (car * c < 2000) {
							inScript.add(next);
							car *= c;
						}
					}
				}
				scripts.add(inScript);
			}
			testScripts.put(d, scripts);
		}
		
		
		/********************************************************************
		 * Fifth: Generate SAQL & Test
		 * 
		 */
		

		//ThreadCluster cluster = new ThreadCluster(1);
		
		for (Dataset d : testScripts.keySet()) {
			BaseDataset devD = dev.getBaseDatasets().get(d.name);
			Set<String> ignore = (ignores.get(d.name) == null) ? new TreeSet<String>() : ignores.get(d.name);
			Set<Field> fieldList = fields.stream()
					.filter(field -> field.parent.compareTo(d) == 0 && !ignore.contains(field.name))
					.collect(Collectors.toSet());
			List<Field> measures = fieldList.stream().filter(field -> field.type == Type.Measure && !ignore.contains(field.name)).collect(Collectors.toList());
			
			for (Set<Field> testFields : testScripts.get(d)) {
				String dims = testFields.stream()
						.map(field -> "'" + field.name + "'")
						.collect(Collectors.joining(", "));
				String meas = measures.stream()
						.map(field -> "sum(" + field.name + ") as " + field.name)
						.collect(Collectors.joining(", "));
				String groupBy = (testFields.size() > 1) ? "(" + dims + ")" : dims;  
				saql = "q = group q by " + groupBy + ";\r\n"
						+ "q = foreach q generate " + dims;
				if (!meas.equals("")) {
					saql +=  ", " + meas;
				}
				if (!ignore.contains("count") && recordCounts) {
					saql += ", count() as RecordCount";
				}
				
				String prodSaql = "q = load \"" + d.getSAQL() + "\";\r\n";
				String devSaql = "q = load \"" + devD.getSAQL() + "\";\r\n";
				if (filters.get(d.name) != null) {
					for (String filter : filters.get(d.name)) {
						devSaql += filter + "\r\n";
						prodSaql += filter + "\r\n";
					}
				}
				devSaql += saql + ";";
				prodSaql +=  saql + ";";
				if (prodSaql.contains("Resolution_Date_Key") || prodSaql.contains("ToDate")) {
					System.err.println("err");
				}
				final String devSaqlFinal = devSaql;
				final String prodSaqlFinal = prodSaql;
				//System.err.println(prodSaql);
				
				cluster.dispatch(new Runnable() {
					public void run() {
						JSONNode prodResults = prod.executeQuery(prodSaqlFinal);
						JSONNode devResults = dev.executeQuery(devSaqlFinal);
						if (prodResults == null) {
							System.out.println("SAQL Error in Prod: " + prodSaqlFinal);
						} else if (devResults == null) {
							System.out.println("SAQL Error in Dev: " + devSaqlFinal);
						} else {
							boolean passed = ResultsTester.doTest(prodResults, devResults, d.name);
							if (passed) {
								String out = "Passed " + d.name + ": (" + testFields.stream().map(field -> field.name).collect(Collectors.joining(", "))
										+ ") [" + measures.stream().map(field -> field.name).collect(Collectors.joining(", ")) + "]";
								System.out.println(out);
							}
						}
					}
				});
			}
		}
		cluster.join();
		
	}
}
