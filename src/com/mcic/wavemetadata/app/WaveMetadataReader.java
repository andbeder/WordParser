package com.mcic.wavemetadata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.filechooser.FileFilter;

import com.mcic.sfrest.SalesforceModel;
import com.mcic.sfrest.SalesforceREST;
import com.mcic.util.CSVAuthor;
import com.mcic.util.RecordSet;
import com.mcic.util.RecordsetOld;
import com.mcic.util.json.Tripple;
import com.mcic.util.json.JSONArray;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONNumber;
import com.mcic.util.json.JSONObject;
import com.mcic.util.json.Pair;
import com.mcic.util.json.ThreadCluster;
import com.mcic.wavemetadata.WaveLineageReader;
import com.mcic.wavemetadata.tool.WaveMetadata;
import com.mcic.wavemetadata.tool.WaveMetadata.BaseDataset;
import com.mcic.wavemetadata.tool.WaveMetadata.Dashboard;
import com.mcic.wavemetadata.tool.WaveMetadata.Dataset;
import com.mcic.wavemetadata.tool.WaveMetadata.Field;
import com.mcic.wavemetadata.tool.WaveMetadata.Recipe;

public class WaveMetadataReader {
	public SalesforceREST agent;
	public WaveLineageReader reader;
	public Map<String, Vector<String>> replicatedDatasetFields;
	public Map<String, JSONNode> recipeDefinitions;
	public String rdiInventoryId;
	
	public RecordSet datasets;
	public RecordSet dashboards;
	public RecordSet dashdatasets;
	public RecordSet dashfields;
	public RecordSet dashlinks;	
	public RecordSet recipes;
	public RecordSet recipeDatasets;
	public RecordSet recipeFields;
	public RecordSet apps;
	public RecordSet pageWidgetStepFields;
	
	public WaveMetadata meta;

	
	public static void main(String[] args) {
		File devFile = null;
		File propFile = null;
		File ignore = null;
		File devFilterFile = null;
		Vector<String> datasets = new Vector<String>();
		String ds = "";
		String curDir = "";
		int i = 0;
		while (i < args.length) {
			String cmd = args[i].toLowerCase();
			switch (cmd) {
			case "-prop": 
				propFile = new File(curDir + args[++i]);
				break;
			}
			i++;
		}
		
		WaveMetadataReader wm = new WaveMetadataReader(propFile);
	}	
	public WaveMetadataReader(File propFile) {
		
		if (propFile == null) {
			JFileChooser fc = new JFileChooser();
			fc.setCurrentDirectory(new File("C:\\Users\\abeder\\git\\JavaUtilities\\MCIC_RDI"));
			fc.setFileFilter(new FileFilter() { 
				public boolean accept(File fileName) {
					if (fileName.isDirectory())
						return true;
					return fileName.getName().endsWith(".properties");
				}
				public String getDescription() {
					return null;
				}});
			
			int res = fc.showOpenDialog(null);
			if (res != JFileChooser.APPROVE_OPTION) {
				System.exit(0);
			}
			propFile = fc.getSelectedFile();
			System.out.println(propFile.getAbsolutePath());
		}
		
		
		recipes = new RecordsetOld();
		recipeDatasets = new RecordsetOld();
		recipeFields = new RecordsetOld();
		dashboards = new RecordsetOld();
		dashdatasets = new RecordsetOld();
		dashfields = new RecordsetOld();
		dashlinks = new RecordsetOld();
		datasets = new RecordsetOld();
		apps = new RecordsetOld();
		pageWidgetStepFields = new RecordsetOld();

		replicatedDatasetFields = null;
		rdiInventoryId = null;
		recipeDefinitions = null;
		
		agent = new SalesforceREST(new SalesforceModel(propFile));
		meta = new WaveMetadata(agent);
		
		//reader = new WaveLineageReader();
		
	}
	
	public void doInventory() {
		int res = JOptionPane.showConfirmDialog(null, "Do you want to write Datasets?");
		boolean writeDatasets = (res == JOptionPane.YES_OPTION) ? true : false;

		res = JOptionPane.showConfirmDialog(null, "Do you want to set COMPASS production fields?");
		boolean setFields = (res == JOptionPane.YES_OPTION) ? true : false;
		
		readDashboards();
		readRecipes();
		if (setFields) {
			setFields();
		}
		readApplications();
		if (writeDatasets) {
			startJob();
		}
	}

//	 ***************************************
//	 *****  Point Recipes to Prod
//	 ***************************************
	
	
	public void setRecipesToProd() {
		if (recipeDefinitions == null) {
			readRecipes();
		}
		
		for (String url : recipeDefinitions.keySet()) {
			JSONNode definition = recipeDefinitions.get(url).get("recipeDefinition");
			String json = definition.toString();
			if (json.indexOf("SFDC_LOCAL") != -1) {
				json = json.replaceAll("SFDC_LOCAL", "COMPASS_Production");
				JSONObject patch = new JSONObject();
				patch.put("recipeDefinition", JSONNode.parse(json));
				System.out.println(patch);
				try {
					FileWriter f = new FileWriter("json.txt");
					f.write(patch.toString());
					f.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				int res = agent.patchJSON(url, patch);
				JSONNode node = agent.getResponse();
				System.out.println(node);
			}
		}
	}
	

//	 ***************************************
//	 *****  Dashboards and Datasets
//	 ***************************************
	
	
	public void readDashboards() {
		//dashboards.clear();
		//dashdatasets.clear();
		//dashfields.clear();
		//dashlinks.clear();
		String[] types = new String[] {"Dashboard", "Lens"};
		String nextURL;

		meta.loadDashboards(null);
		Collection<Dashboard> dList = meta.getDashboards().values();

		for (Dashboard d : dList) {
			Set<String> dpwsf = new TreeSet<String>();			
			
			dashboards.add("DashboardName", d.name);
			dashboards.add("Application", d.appName);
			dashboards.add("MasterLabel", d.label);
			dashboards.add("Id", d.id);
			dashboards.add("Type", d.type);
			dashboards.add("CreatedBy", d.createdBy);
			dashboards.add("LastModifiedBy", d.lastModifiedBy);
			dashboards.next();
			
			for (BaseDataset bd : d.datasets) {
				dashdatasets.add("DashboardName", d.name);
				dashdatasets.add("DatasetName", bd.name);
				dashdatasets.next();
			}

			for (Field field : d.fields) {
				dashfields.add("DashboardName", d.name);
				dashfields.add("DatasetName", field.parent.name);
				dashfields.add("FieldName", field.name);
				dashfields.next();
			}
			
			for (String link : d.links) {
				dashlinks.add("Dashboard Name", d.name);
				dashlinks.add("Link Name", link);
				dashlinks.next();
			}
			
			for (Entry<String, List<String>> page : d.pageToWidgets.entrySet()) {
				for (String widget : page.getValue()) {
					String step = d.widgetToStep.get(widget);
					if (step != null) {
						for (String field : d.stepToFields.get(step)) {
							String key = d.name + "~" + page.getKey() + "~" + widget + "~" + step + "~" + field;
							if (!dpwsf.contains(key)) {
								dpwsf.add(key);
								pageWidgetStepFields.add("Dashboard", d.name);
								pageWidgetStepFields.add("Page", page.getKey());
								pageWidgetStepFields.add("Widget", widget);
								pageWidgetStepFields.add("Step", step);
								pageWidgetStepFields.add("Field", field);
								pageWidgetStepFields.next();
							}
						}
					}
				}
			}
		}
	}
	
	public void saveDashboards() {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("Reports.csv"));
			out.write(dashboards.toString());
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		agent.writeDataset("Dashboards", "Dashboards", "RDI_Inventory", dashboards, null, null);
		agent.writeDataset("DashboardDatasetJunction", "DashboardDatasetJunction", "RDI_Inventory", dashdatasets, null, null);
		agent.writeDataset("DashboardFields", "DashboardFields", "RDI_Inventory", dashfields, null, null);
		agent.writeDataset("DashboardLinks", "DashboardLinks", "RDI_Inventory", dashlinks, null, null);
		agent.writeDataset("PageWidgetStepFields", "PageWidgetStepFields", "RDI_Inventory", pageWidgetStepFields, null, null);
	}
	
	
//	***************************************
//	*****  Datasets
//	***************************************


	public void readDatasets() {
		meta.loadDatasets(null);
//		datasets.clear();
		for (String dsName : meta.getBaseDatasets().keySet()) {
			Dataset ds = meta.getDataset(dsName);
			datasets.add("DatasetName", dsName);
			datasets.add("MasterLabel", ds.label);
			datasets.add("Application", ds.application);
			datasets.next();
		}
	}
	
	public void saveDatasets() {
		agent.writeDataset("Datasets", "Datasets", "RDI_Inventory", datasets, null, null);
	}
	
		
//		***************************************
//		*****  Recipes
//		***************************************

	
	public void readRecipes() {
//		recipes.clear();
//		recipeDatasets.clear();
//		recipeFields.clear();
		
		meta.readRecipes();
		Map<String, Long> recipeLastUpdate = new TreeMap<String, Long>();
		Map<Field, Tripple<String>> fields = new TreeMap<Field, Tripple<String>>(); 
		Map<Field, Integer> nextRun = new TreeMap<Field, Integer>();

		for (Recipe recipe : meta.getRecipes().values().stream()
				.filter((recipe) -> recipe.label != null).collect(Collectors.toList())) {
			String recipeName = recipe.name;
			recipes.add("RecipeName", recipeName);
			recipes.add("MasterLabel", recipe.label);
			recipes.add("Id", recipe.id);
			recipes.add("Schedule", recipe.nextSchedule);
			recipes.next();
			long thisRun = 0;
			if (recipe.nextSchedule != null) {
				DateFormat df = new SimpleDateFormat();
				try {
					thisRun = df.parse(recipeName).getTime();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (recipe.label.equals("RDI Inventory")){
				rdiInventoryId = recipe.id;
			}
			
		//  <recipeName/"Dataset", nodeName/datasetName, fieldName>
			
			//  Analyze all 'save' nodes to populate initial field swarm
			final long thisRunFin = thisRun;
			recipe.nodes.values().stream().filter((node) -> node.get("action").asString().equals("save")
					&& node.get("parameters").get("dataset").get("type").equals("analyticsDataset")).forEach((node) -> {
						JSONNode d = node.get("parameters").get("dataset");
						String datasetName = d.get("name").asString();
						Dataset ds = meta.getDataset(datasetName);
						recipeDatasets.add("DatasetName", datasetName);
						recipeDatasets.add("RecipeName", recipeName);
						recipeDatasets.add("Action", "save");
						recipeDatasets.next();
						
						//  Add all dataset fields to 'save'
						for (Field f : ds.fields.values()) {
							if (fields.containsKey(f)) {
								String thatRecipe = fields.get(f).first;
								long thatRun = recipeLastUpdate.get(thatRecipe) == null ? 0 : recipeLastUpdate.get(thatRecipe);
								if (thisRunFin > thatRun) {
									fields.put(f, new Tripple(recipeName, datasetName, f.name));
								}
							} else {
								fields.put(f, new Tripple(recipeName, datasetName, f.name));
							}
						}
			});
			
			//  Perform top-down field cascade for reference field names
			Map<String, Map<String, Set<Pair<String, String>>>> nodeFields = new TreeMap<String, Map<String, Set<Pair<String, String>>>>(); //  Map<nodeName, Map<fieldName, <sourceNode, sourceField>>
			LinkedList<String> remaining = new LinkedList<String>();

			//recipe.nodes.entrySet().stream().filter((set) -> set.getValue().get("action").asString().equals("load")).forEach((set) -> {
			for (Entry<String, JSONNode> set : recipe.nodes.entrySet()) {
				String name = set.getKey();
				JSONNode node = set.getValue();
				if (node.get("action").equals("load")) {
					final Map<String, Set<Pair<String, String>>> nf = nodeFields.put(name, new TreeMap<String, Set<Pair<String, String>>>());
					JSONNode ds = node.get("parameters").get("dataset");
					String type = ds.get("type").asString();
					String sourceName = type.equals("analyticsDataset") ? ds.get("name").asString() : "SF: " + ds.get("sourceObjectName").asString(); 
					node.get("parameters").get("fields").values().stream().map((n) -> n.asString()).forEach((fieldName) -> {
						Set<Pair<String, String>> s = nf.put(fieldName, new TreeSet<Pair<String, String>>()); 
						s.add(new Pair<String, String>(sourceName, fieldName));
					});
				} else {
					remaining.add(name);
				}
			}
			
			while (remaining.size() > 0) {
				LinkedList<String> nodeNames = (LinkedList<String>) remaining.clone();
				for (String nodeName : nodeNames) {
					JSONNode node = recipe.nodes.get(nodeName);
					String action = node.get("action").asString();
					List<String> sources = node.get("sources").values().stream().map((json) -> json.asString()).collect(Collectors.toList());
					boolean complete = true;
					for (String source : sources) {
						complete = nodeFields.containsKey(source) ? complete : false; 
					}
					
					if (complete) {
						final Map<String, Set<Pair<String, String>>> fieldMap = nodeFields.get(nodeName) != null ? nodeFields.get(nodeName) : nodeFields.put(nodeName, new TreeMap<String, Set<Pair<String, String>>>());
						if (fieldMap == null) {
							System.out.println("ododps");
						}
						JSONNode p = node.get("parameters");
						String firstSource = sources.size() > 0 ? sources.get(0) : null;
						for (String source : sources) {
							//for (String : )
							for (String fieldName : nodeFields.get(source).keySet()) {
								Set<Pair<String, String>> s = fieldMap.put(fieldName, new TreeSet<Pair<String, String>>());
								s.add(new Pair<String, String>(source, fieldName));
							}
						}
						switch (action) {
						case "schema":
							if (p.contains("slice")) {
								JSONNode s = p.get("slice");
								Stream<String> fieldStr = s.get("fields").values().stream().map((n) -> n.asString());
								if (s.get("mode").asString().equals("DROP")) {
									fieldStr.forEach((field) -> fieldMap.remove(field));
								} else { //  mode == "SELECT"
									fieldMap.clear();
									fieldMap.putAll(fieldStr.collect(Collectors.toMap((fieldName) -> fieldName, (fieldName) -> {
										Set<Pair<String, String>> sp = new TreeSet<Pair<String, String>>();
										sp.add(new Pair<String, String>(firstSource, fieldName));
										return sp;
									})));
								}
							} else {
								for (JSONNode f : p.get("fields").values()) {
									String fieldName = f.get("name").asString();
									String thisName = f.get("newProperties").get("name").asString();
									fieldMap.remove(fieldName);
									Set<Pair<String, String>> sp = fieldMap.containsKey(thisName) ? fieldMap.get(thisName) : fieldMap.put(thisName, new TreeSet<Pair<String, String>>());
									sp.add(new Pair<String, String>(firstSource, fieldName));
								}
							}
							break;
							
						case "formula":
							if (p.get("expressionType").equals("SQL")) {
								for (JSONNode f : p.get("fields").values()) {
									String newName = f.get("name").asString();
									//  Find any old names in the formula
									String formula = f.get("formulaExpression").asString();
									formula.replaceAll("\\(|\\)|'|\"", " ");
									String[] words = formula.split(" ");
									Set<Pair<String, String>> found = new TreeSet<Pair<String, String>>();
									fieldMap.put(newName, found);
									for (String word : words) {
										if (fieldMap.containsKey(word)) {
											Set<Pair<String, String>> newSource = fieldMap.get(word);
											found.addAll(newSource);
										}
									}
								}
							}
							break;
							
						case "join":
							String leftSource = sources.get(0);
							String rightSource = sources.get(1);
							String qual = p.get("rightQualifier").asString();
							
							fieldMap.clear();
							for (String fieldName : nodeFields.get(leftSource).keySet()) {
								Set<Pair<String, String>> s = fieldMap.containsKey(fieldName) ? fieldMap.get(fieldName) : fieldMap.put(fieldName, new TreeSet<Pair<String, String>>());
								s.add(new Pair<String, String>(leftSource, fieldName));
							}
							
							Set<String> ignore = new TreeSet<String>();
							if (node.get("schema") != null) {
								ignore.addAll(node.get("schema").get("slice").get("fields").values().stream()
								.map((json) -> json.asString()).collect(Collectors.toSet()));
							}
							
							for (String fieldName : nodeFields.get(rightSource).keySet()) {
								String newName = qual + "." + fieldName;
								Set<Pair<String, String>> s = fieldMap.containsKey(newName) ? fieldMap.get(newName) : fieldMap.put(newName, new TreeSet<Pair<String, String>>());
								s.add(new Pair<String, String>(rightSource, fieldName));
							}
						}
					}
				}
			}
			
			//  Analyze all 'load' connected nodes to identify the fields in question
			Stream<JSONNode> nodes = recipe.nodes.values().stream().filter((node) -> node.get("action").asString().equals("load"));
//					&& node.get("parameters").get("dataset").get("type").equals("connectedDataset")).forEach((node) -> {
			nodes.forEach((node) -> {
				JSONNode d = node.get("parameters").get("dataset");
				String datasetName = d.get("name").asString();
				Dataset ds = meta.getDataset(datasetName);
				recipeDatasets.add("DatasetName", datasetName);
				recipeDatasets.add("RecipeName", recipeName);
				recipeDatasets.add("Action", "save");
				recipeDatasets.next();
				for (JSONNode connField : node.get("fields").values()) {
					String field = connField.asString();
					recipeFields.add("RecipeName", recipeName);
					recipeFields.add("ObjectName", d.get("sourceObjectName").asString());
					recipeFields.add("FieldName", field);
					recipeFields.next();
				}
			});
		}
		
		
	}
	
	public void saveRecipes() {
		agent.writeDataset("Recipes", "Recipes", "RDI_Inventory", recipes, null, null);
		agent.writeDataset("RecipeDatasets", "RecipeDatasets", "RDI_Inventory", recipeDatasets, null, null);
		agent.writeDataset("RecipeFields", "RecipeFields", "RDI_Inventory", recipeFields, null, null);
	}
	
	public void setFields() {
//		*************************************************
//		****  Set fields of COMPASS Production objects
//		*************************************************

		
		String prodConnector = null;
		String localConnector = null;
		if (replicatedDatasetFields == null) {
			readRecipes();
		}
		
		Map<String, Map<String, String>> localJSON = new TreeMap<String, Map<String, String>>();
		Map<String, String> prodURLs = new TreeMap<String, String>();
		String nextURL = "/services/data/v58.0/wave/replicatedDatasets";
		int res = agent.get(nextURL, null);
		JSONNode r = agent.getResponse();
		for (JSONNode node : r.get("replicatedDatasets").values()) {
			JSONObject rd = (JSONObject)node;
			String label = rd.get("connector").get("label").asString();
			String fieldsURL = rd.get("fieldsUrl").asString();
			String name = node.get("sourceObjectName").asString();
			if (label.equals("COMPASS Production")) {
				prodConnector = rd.get("connector").get("id").asString();
				prodURLs.put(name, fieldsURL);
			}
			if (label.equals("SFDC_LOCAL")) {
				localConnector = rd.get("connector").get("id").asString();
			}
		}

		for (String objectName : replicatedDatasetFields.keySet()) {
			String url = prodURLs.get(objectName);
			if (url == null) {
				System.out.println(objectName);
			}
		}			
		
		CSVAuthor a = null;
		try {
			a = new CSVAuthor(new File("fields.csv"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (String objectName : replicatedDatasetFields.keySet()) {
			JSONObject root = new JSONObject();
			JSONArray fields = root.addArray("fields");
			String fieldsURL = prodURLs.get(objectName);
			if (fieldsURL == null) {
				JSONObject dataset = new JSONObject();
				//dataset.addString("connection​Mode", "PeriodicFull");
				dataset.addString("connectorId", prodConnector);
				//dataset.addBoolean("full​Refresh", true);
				dataset.addString("sourceObjectName", objectName);
				/*
				JSONNode newDataset = agent.postJSON("/services/data/v58.0/wave/replicatedDatasets", dataset);
				String id = newDataset.get("id").asString();
				fieldsURL = "/services/data/v58.0/wave/replicatedDatasets/" + id + "/fields";
				prodURLs.put(objectName, fieldsURL);
				*/
			}
			
			//  Get field definitions from SF Org
			Map<String, String> fieldJSON = new TreeMap<String, String>();
			nextURL = "/services/data/v58.0/wave/dataConnectors/" + localConnector + "/sourceObjects/" + objectName + "/fields";
			res = agent.get(nextURL, null);
			JSONNode fieldList = agent.getResponse();
			for (JSONNode field : fieldList.get("fields").values()) {
				String name = field.get("name").asString();
				String json = field.toCompressedString();
				fieldJSON.put(name,  json);
				a.setValue("objectName", objectName);
				a.setValue("fieldName", name);
				//a.setValue("fieldJSON", json);
				if (field.get("multiValue") == null) {
					a.setValue("isMultivalue", "false");
					a.setValue("multiValueSeparator", "");
				} else {
					a.setValue("isMultivalue", field.get("multiValue").asBoolean() ? "true" : "false");
					if (field.get("multiValueSeparator") != null) {
						a.setValue("multiValueSeparator", field.get("multiValueSeparator").asString());
					}
				}
				a.writeValues();
				if (name.equals("VAP_Entity__c")) {
					System.out.println("Trap");
				}
			}
			
			//Map<String, String> fieldJSON = localJSON.get(objectName);
			if (fieldJSON != null) {
				for (String fieldName : replicatedDatasetFields.get(objectName)) {
					String json = fieldJSON.get(fieldName);
					if (json != null) {
						JSONNode o = JSONNode.parse(json);
						JSONNumber n = (JSONNumber)o.get("precision");
						if (n != null && n.asDouble() > 32000) {
							n.setValue(32000);
						}
						fields.add(o);
					}
				}					
				//agent.patchJSON(fieldsURL, root);
			}
		}
		a.close();

	}

		
//		***************************************
//		*****  Applications
//		***************************************

	public void readApplications() {
		
//		apps.clear();
		String nextURL = "/services/data/v58.0/wave/folders" ;
		while (nextURL != null && !nextURL.equals("null")) {
			int res = agent.get(nextURL, null);
			JSONNode root = agent.getResponse();
			for (JSONNode o : root.get("folders").values()) {
				apps.add("AppName", o.get("name").asString());
				apps.add("AppLabel", o.get("label").asString());
				apps.next();
			}
			nextURL = root.get("nextPageUrl").asString();
		}
	}
	
	public void saveApplications() {
		agent.writeDataset("Applications", "Applications", "RDI_Inventory", apps, null, null);
	}

//		***************************************
//		*****  Connected Objects
//		***************************************

/*		
		CSVAuthor conn = new CSVAuthor();
		conn.setHeader(new String[]{"ObjectName","ConnectorType","FieldName","FieldLabel"});
		nextURL = "/services/data/v58.0/wave/replicatedDatasets" ;
		while (nextURL != null && !nextURL.equals("null")) {
			JSONObject root = agent.get(nextURL, null);
			for (JSONNode o : root.get("replicatedDatasets").values()) {
				String name = o.get("name").asString();
				String type = ((JSONObject)o.get("connector")).get("name").asString();
				String fieldsURL = o.get("fieldsUrl").asString();
				JSONObject replicatedFields = agent.get(fieldsURL, null);
				if (replicatedFields != null) {
					for (JSONNode rfo : replicatedFields.get("fields").values()) {
							String fname = rfo.get("name").asString();
							String flabel = (rfo.get("label") == null) ? fname : rfo.get("label").asString();
							conn.setValue("ObjectName", name);
							conn.setValue("ConnectorType", type);
							conn.setValue("FieldName", fname);
							conn.setValue("FieldLabel", flabel);
							conn.writeValues();
					}
				}
			}
			nextURL = root.get("nextPageUrl").asString();
		}
		if (writeDatasets) {
			agent.writeDataset("ReplicatedObjects", "ReplicatedObjects", "RDI_Inventory", conn);
			agent.startJob(rdiInventoryId);
		}
*/		

	public void startJob() {
		if (rdiInventoryId == null) {
			readRecipes();
		}
		agent.startJob(rdiInventoryId);
	}
}

