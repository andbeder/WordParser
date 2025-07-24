package com.mcic.wavemetadata.tool;

import java.awt.Component;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
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

import org.unbescape.html.HtmlEscape;

import com.mcic.sfrest.SalesforceREST;
import com.mcic.util.json.JSONArray;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.util.json.ThreadCluster;


import com.mcic.util.json.Tripple;

public class WaveMetadata {
	public final static String EXECUTE_QUERY = "/services/data/v58.0/wave/query";
	public final static int CONCURRENCY = 10;
	
	public enum Type {
		Dimension, Measure, Date
	};
	
	/********************************************************
	 * Class definitions. 
	 * 	  BaseDashboard / Dashboard: Encapsulates WaveDashboard https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_rest.meta/bi_dev_guide_rest/bi_resources_dashboards_id.htm
	 *    BaseDataset / Dataset: WaveDataset https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_rest.meta/bi_dev_guide_rest/bi_resources_dataset_id.htm
	 */

	public static class Field implements Comparable<Field> {
		public Type type;
		public String name;
		public String label;
		public String alias;
		public boolean isMultiValue;
		public Dataset parent;
		public String lens;
		public String tab;

		private Field(Type type, String name, String label, String alias, Dataset parent, boolean isMultivalue) {
			this.type = type;
			this.name = name;
			this.label = label;
			this.alias = (alias == null) ? name : alias;
			this.parent = parent;
			this.isMultiValue = isMultivalue;
		}

		@Override
		public int compareTo(Field f) {
			String n1 = parent.name + "~" + name;
			String n2 = f.parent.name + "~" + f.name;
			return n1.compareTo(n2);
		}

		@Override
		public String toString() {
			return name;
		}
	}

	public static class BaseDataset implements Comparable<BaseDataset> {
		public String name;
		public String label;
		public String id;
		public String versionId;
		public String url;
		public String application;

		public BaseDataset() {
			
		}
		
		public String getSAQL() {
			return id + "/" + versionId;
		}

		@Override
		public int compareTo(BaseDataset d) {
			return name.compareTo(d.name);
		}

		@Override
		public String toString() {
			return name;
		}
	}

	public static class Dataset extends BaseDataset{
		public Map<String, Field> fields;

		private Dataset(BaseDataset b) {
			name = b.name;
			label = b.label;
			id = b.id;
			versionId = b.versionId;
			url = b.url;
			application = b.application;
		}
		
		public Field getField(String name) {
			return fields.get(name);
		}

		@Override
		public String toString() {
			return name;
		}
	}

	public static class BaseDashboard implements Comparable<BaseDashboard> {
		public String name;
		public String label;
		public String id;
		public String url;
		public String createdBy;
		public String lastModifiedBy;
		public String appName;
		public String type;

		private BaseDashboard(JSONNode asset) {
			name = asset.get("name").asString();
			id = asset.get("id").asString();
			label = asset.get("label").asString();
			url = asset.get("url").asString();
			createdBy = asset.get("createdBy").get("name").asString();
			lastModifiedBy = asset.get("lastModifiedBy").get("name").asString();
			appName = asset.get("folder").get("label").asString();
		}
		
		private BaseDashboard(BaseDashboard b) {
			name = b.name;
			label = b.label;
			id = b.id;
			url = b.url;
			type = b.type;
			createdBy = b.createdBy;
			lastModifiedBy = b.lastModifiedBy;
			appName = b.appName;
		}

		public int compareTo(BaseDashboard d) {
			return name.compareTo(d.name);
		}
		
		@Override
		public String toString() {
			return name;
		}
	}

	public static class Dashboard extends BaseDashboard {
		//public JSONNode state;
		//public JSONNode datasets;
		public Collection<BaseDataset> datasets;
		public Collection<Field> fields;
		public Collection<String> links;
		public Map<String, List<String>> pageToWidgets ;
		public Map<String, String> widgetToStep;
		public Map<String, List<String>> stepToFields;

		private Dashboard(BaseDashboard b) {
			super(b);
			datasets = new LinkedList<BaseDataset>();
			fields = new TreeSet<Field>();
			pageToWidgets = new TreeMap<String, List<String>>();
			widgetToStep = new TreeMap<String, String>();
			stepToFields = new TreeMap<String, List<String>>();
		}
		
		@Override
		public String toString() {
			return name;
		}
	}
	
	public static class Recipe {
		public String name;
		public String label;
		public String id;
		public boolean isScheduled;
		public String nextSchedule;
		public String url;
		public Map<String, JSONNode> nodes;
		public Set<Tripple<String>> saves;  //  <Node Name>, <Dataset Name>, <Field API Name>
		public Set<Tripple<String>> loads;  //  <Node Name>, <Dataset Name>, <Field API Name>
		
		public Recipe(String name, String label) {
			this.name = name;
			this.label = label;
			saves = new TreeSet<Tripple<String>>();
			loads = new TreeSet<Tripple<String>>();
		}
	}
	
	public class Dataflow {
		public String id;
		public String name;
		public String label;
		public Dataflow(String id, String name, String label) {
			super();
			this.id = id;
			this.name = name;
			this.label = label;
		}		
	}

	/****************************************
	 * Wave Metadata Class
	 */

	private SalesforceREST agent;
	private Map<String, BaseDataset> baseDatasets;
	private Map<String, BaseDashboard> baseDashboards;
	private Map<String, Dataset> datasets;
	private Map<String, Dashboard> dashboards;
	private Map<String, Recipe> recipes;
	private Map<String, Instance> instances;
	private List<String> currentFieldList;
	private Map<String, Element> elements;
	private List<Source> dataElementSources;
	private Map<String, Dataflow> dataflows;
	private Map<String, String> ownerMap;

	public WaveMetadata(SalesforceREST agent) {
		this.agent = agent;
		datasets = new ConcurrentHashMap<String, Dataset>();
		dashboards = new ConcurrentHashMap<String, Dashboard>();
		baseDashboards = new TreeMap<String, BaseDashboard>();
		baseDatasets = new TreeMap<String, BaseDataset>();
		recipes = new TreeMap<String, Recipe>();
		instances = new TreeMap<String, Instance>(); 
		dataElementSources = new LinkedList<Source>();
		dataflows = new TreeMap<String, Dataflow>();
		ownerMap = new TreeMap<String, String>();
		elements = new TreeMap<String, Element>();
		
		readDatasets();
		readDashboards("dashboards");
		readDashboards("lenses");
	}
	
	

	public SalesforceREST getAgent() {
		return agent;
	}
	
	public Map<String, Recipe> getRecipes(){
		return recipes;
	}

	public Map<String, BaseDataset> getBaseDatasets() {
		return baseDatasets;
	}


	public Map<String, BaseDashboard> getBaseDashboards() {
		return baseDashboards;
	}
	
	public Map<String, Dashboard> getDashboards(){
		return dashboards;
	}
	
	public Dataset getDataset(String name) {
		Dataset d = datasets.get(name);
		if (d == null) {
			BaseDataset bd = baseDatasets.get(name);
			if (bd != null) {
				d = loadDataset(bd);
				datasets.put(name, d);
			}
		}
		return d;
	}
	
	public void loadDatasets(Collection<String> dsNames) {
		if (dsNames == null) {
			dsNames = baseDatasets.keySet();
		}
		
		ThreadCluster tc = new ThreadCluster(CONCURRENCY);
		for (String name : dsNames) {
			BaseDataset bd = baseDatasets.get(name);
			if (bd != null) {
				tc.dispatch(new Runnable() {
					public void run() {
						Dataset d = loadDataset(bd);
						datasets.put(name, d);
					}
				});
			}
		}
		tc.join();
	}
	
	public void loadDashboards(Collection<String> names) {
		if (names == null) {
			names = baseDashboards.keySet();
		}
		ThreadCluster tc = new ThreadCluster(CONCURRENCY);
		for (String name : names) {
			BaseDashboard bd = baseDashboards.get(name);
			if (bd != null) {
				tc.dispatch(new Runnable() {
					public void run() {
						Dashboard d = loadDashboard(bd);
						dashboards.put(name, d);
					}
				});
			}
		}
		tc.join();
	}
	
	private void readDatasets() {
		String nextURL = "/services/data/v58.0/wave/datasets";
		while (nextURL != null && !nextURL.equals("null")) {
			int res = agent.get(nextURL, null);
			JSONObject root = (JSONObject) agent.getResponse();
			for (JSONNode o : root.get("datasets").values()) {
				BaseDataset ds = new BaseDataset();
				ds.name = o.get("name").asString();
				ds.id = o.get("id").asString();
				ds.label = o.get("label").asString();
				ds.application = o.get("folder").get("name") == null 
						? o.get("folder").get("label").asString() 
						: o.get("folder").get("name").asString();
				if (ds.application == null) {
					System.out.println("Issue");
				}
				ds.url = (o.get("currentVersionUrl") == null) ? null : o.get("currentVersionUrl").asString();
				ds.versionId = (o.get("currentVersionId") == null) ? null : o.get("currentVersionId").asString();
				
				baseDatasets.put(ds.name, ds);
			}
			nextURL = root.get("nextPageUrl").asString();
		}
	}

	private void readDashboards(String typePlural) {
		String type = typePlural.equals("dashboards") ? "Dashboard" : "Lens";
		String nextURL = "/services/data/v58.0/wave/" + typePlural + "?pageSize=200";
		while (nextURL != null && !nextURL.equals("null")) {
			int res = agent.get(nextURL, null);
			JSONObject root = (JSONObject) agent.getResponse();
			for (JSONNode asset : root.get(typePlural).values()) {
				BaseDashboard bd = new BaseDashboard(asset);
				baseDashboards.put(bd.name, bd);
				//System.err.println("Loading Dashboard:" + ds.name);
			}
			//System.err.println("New Page");
			nextURL = root.get("nextPageUrl").asString();
			if (nextURL != null && !nextURL.equals("null")) {
				nextURL +=  "&pageSize=200";
			}
		}
	}

	private Dataset loadDataset(BaseDataset bd) {
		Dataset ds = new Dataset(bd);
		if (ds.url != null) {
			ds.fields = new TreeMap<String, Field>();
			Map<String, Field> fields = ds.fields;
			Vector<Field> dates = new Vector<Field>();
			int res = agent.get(ds.url, null);
			JSONObject root = (JSONObject) agent.getResponse();
			JSONNode xmd = root.get("xmdMain");
			for (JSONNode date : xmd.get("dates").values()) {
				JSONNode apiNames = date.get("fields");
				String name = apiNames.get("day").asString();
				name = name.substring(0, name.length() - 4);
				Field f = new Field(Type.Date, name, date.get("label").asString(), null, ds, false);
				fields.put(f.name, f);
				dates.add(f);
			}
			for (JSONNode mea : xmd.get("measures").values()) {
				Field f = new Field(Type.Measure, mea.get("field").asString(), mea.get("label").asString(), null, ds, false);
				fields.put(f.name, f);
				boolean isDate = false;
				for (Field d : dates) {
					if (f.name.startsWith(d.name)) {
						isDate = true;
					}
				}
				//if (f.name.contains("Date_of_Loss")) {
				//System.out.println("Ouch");
				//}
				
				if (!isDate) {
					if (f.name.contains("_epoch")) { 
						System.out.println("Check"); 
						}
					fields.put(f.name, f);
				}
			}
			for (JSONNode dim : xmd.get("dimensions").values()) {
				boolean isMultivalue = dim.get("isMultiValue") == null ? false : dim.get("isMultiValue").asBoolean();
				Field f = new Field(Type.Dimension, dim.get("field").asString(), dim.get("label").asString(), null, ds, isMultivalue);
//				if (f.name.contains("Begin_21st")) {
//					System.out.println("Ouch");
//				}
				boolean isDate = false;
				for (Field d : dates) {
					if (f.label.startsWith(d.name)) {
						isDate = true;
					}
				}
				if (!isDate) {
					if (f.name.contains("_epoch")) { 
						System.out.println("Check"); 
						}
					fields.put(f.name, f);
				}
			}
		}
		return ds;
	}

	private Dashboard loadDashboard(BaseDashboard bd) {
		int res = agent.get(bd.url, null);
		JSONObject root = (JSONObject) agent.getResponse();
		Dashboard d = new Dashboard(bd);
		JSONNode state = root.get("state");
		JSONNode datasets = root.get("datasets");
		Set<BaseDataset> dList = new TreeSet<BaseDataset>();
		for (JSONNode n : datasets.values()) {
			BaseDataset ds = this.baseDatasets.get(n.get("name").asString());
			if (ds != null) {
				dList.add(ds);
			}
		}
		d.datasets = dList;
		d.fields = readDashboardFields(d, state);
		
		return d;
	}

	private void addField(String datasetName, String fieldName, Set<Field> output) {
		if (fieldName.startsWith("rollup(")) {
			fieldName = fieldName.substring(7, fieldName.length() - 1);
		}
		if (datasetName != null) {
			Dataset d = getDataset(datasetName);
			if (d != null) {
				List<String> fields = new LinkedList<String>();
				if (fieldName.indexOf(",") >= 0) {
					fields = Arrays.asList(fieldName.split(",")).stream()
							.map((field) -> field.trim())
							.collect(Collectors.toList());
				} else {
					fields.add(fieldName);
				}
				for (String field : fields) {
					Field f = d.getField(field);
					if (f != null) {
						output.add(f);
						currentFieldList.add(field);
					}
				}
			}
		}
	}
	
	public void readFilter(String datasetName, Set<Field> output, JSONNode filter) {
		if (filter.getType() == JSONNode.Type.ARRAY) {
			if (filter.elementAt(0).getType() == JSONNode.Type.ARRAY) {
				for (JSONNode element : filter.values()) {
					readFilter(datasetName, output, element);
				}
			} else {
				String fieldName = filter.elementAt(0).asString();
				addField(datasetName, fieldName, output);
			}
		}
	}

	public Set<Field> readDashboardFields(Dashboard dash, JSONNode state) {
		Set<Field> output = new TreeSet<Field>();
		dash.links = new LinkedList<String>();
				
		/******************
		 *   Read all of the widgets looking for links to other dashboards
		 */
		
		JSONNode widgets = state.get("widgets");
		for (Entry<String, JSONNode> widgetSet : widgets.entrySet()) {
			JSONNode widget = widgetSet.getValue();
			String name = widgetSet.getKey();
			String source = widget.get("parameters").get("step") == null ? null : widget.get("parameters").get("step").asString();
			dash.widgetToStep.put(name, source);
			if (widget.get("type").asString().equals("link")) {
				if (widget.get("parameters").get("destinationType").asString().equals("dashboard")) {
					String dName = widget.get("parameters").get("destinationLink").get("name").asString();
					dash.links.add(dName);
				}
			}
		}

		/******************
		 *   Read all of the pages cataloging widgets
		 */
		
		if (state.get("gridLayouts").size() > 0) {
			JSONNode pages = state.get("gridLayouts").elementAt(0).get("pages");
			for (JSONNode page : pages.values()) {
				String pName = page.get("label") == null ? "Default" : page.get("label").asString();
				for (JSONNode widget : page.get("widgets").values()) {
					String wName = widget.get("name").asString();
					List<String> wList = dash.pageToWidgets.get(pName);
					if (wList == null) {
						wList = new LinkedList<String>();
						dash.pageToWidgets.put(pName, wList);
					}
					wList.add(wName);
				}
			}
		}
		
		JSONNode steps = state.get("steps");
		for (Entry<String, JSONNode> stepSet : steps.entrySet()) {
			JSONNode step = stepSet.getValue();
			currentFieldList = new LinkedList<String>();
			dash.stepToFields.put(stepSet.getKey(), currentFieldList);
			
			String type = step.get("type").asString();
			if (type.equals("staticflex")) {
				// Do nothing
			} else if (type.equals("aggregateflex") || type.equals("grain")) {
				JSONNode query = step.get("query");
				JSONNode sourceFilters = query.get("sourceFilters");
				if (sourceFilters != null) {
					for (Entry<String, JSONNode> entry : sourceFilters.entrySet()) {
						String datasetName = entry.getKey();
						for (JSONNode filter : entry.getValue().get("filters").values()) {
							readFilter(datasetName, output, filter);
						}
					}
				}
				
				JSONNode sources = query.get("sources");
				if (sources != null) {
					for (JSONNode source : sources.values()) {
						if (source.get("name") != null) {
							String datasetName = source.get("name").asString();
	
							// columns
							for (JSONNode column : source.get("columns").values()) {
								if (column.get("field") != null) {
									JSONNode fieldJSON = column.get("field");
									if (fieldJSON.getType() == JSONNode.Type.STRING) {
										addField(datasetName, fieldJSON.asString(), output);
									} else if (fieldJSON.values().size() == 2) {
										String fieldName = fieldJSON.elementAt(1).asString();
										addField(datasetName, fieldName, output);
									} else if (fieldJSON.values().size() > 2) {
										String fieldName = fieldJSON.elementAt(2).asString();
										addField(datasetName, fieldName, output);
									}
								}
							}
	
							// filters
							for (JSONNode filter : source.get("filters").values()) {
								String fieldName = filter.elementAt(0).asString();
								addField(datasetName, fieldName, output);
							}
	
							// groups
							JSONNode groups = source.get("groups");
							if (groups != null && groups.getType() == JSONNode.Type.ARRAY) {
								for (JSONNode groupNode : source.get("groups").values()) {
									String fieldName = groupNode.asString();
									addField(datasetName, fieldName, output);
								}
							}
						}
					}
				} else {
					String datasetName = null;
					JSONNode ds = (JSONArray) step.get("datasets");
					for (JSONNode n : ds.values()) {
						datasetName = n.get("name").asString();
					}
					if (query.get("query") != null) {
						JSONNode n = query.get("query");
						String queryJSON = n.asString();
						queryJSON = queryJSON.replace("\\\"", "\"");
						queryJSON = queryJSON.replace("&quot;", "\"");
						queryJSON = queryJSON.replace("&#39;", "'");
						queryJSON = queryJSON.replace("&#92;", "\\");
						JSONNode q = JSONNode.parse(queryJSON);
						if (q.get("measures") != null)
							for (JSONNode measures : q.get("measures").values()) {
								String fieldName = measures.elementAt(1).asString();
								addField(datasetName, fieldName, output);
							}
						if (q.get("values") != null)
							for (JSONNode value : q.get("values").values()) {
								String fieldName = value.asString();
								addField(datasetName, fieldName, output);
							}
						if (q.get("groups") != null) {
							for (JSONNode group : q.get("groups").values()) {
								if (group.getClass().getSimpleName().equals("JSONArray")) {
									for (JSONNode gn : group.values()) {
										String fieldName = gn.asString();
										addField(datasetName, fieldName, output);
									}
								} else {
									String fieldName = group.asString();
									addField(datasetName, fieldName, output);
								}
							}
						}
					}
				}
			} else {
				if (type.equals("saql")) {					
					String escSaql = step.get("query").asString();
					String saql = HtmlEscape.unescapeHtml(escSaql);
					saql = saql.replace("\\n", "");
					saql = saql.replaceAll("\\{\\{.*?\\}\\}", "");
					String[] lines = saql.split(";");
					TreeMap<String, String> dataSets = new TreeMap<String, String>();
					for (String line : lines) {
						if (!line.startsWith("--") && line.indexOf("=") > 0) {
							String rs = line.substring(0, line.indexOf("=")).trim();
							String rest = line.substring(line.indexOf("=") + 1).trim();
							String cmd = rest.substring(0, rest.indexOf(" ")).toLowerCase();
							String target = dataSets.get(rs);
							rest = rest.substring(cmd.length() + 1);
							switch (cmd) {
							case "load":
								target = rest.substring(rest.indexOf('"') + 1, rest.indexOf('"', rest.indexOf('"') + 1));
								dataSets.put(rs, target);
								break;
							case "group":
							{
								String stream = rest.substring(0, rest.indexOf(" "));
								String source = dataSets.get(stream);
								if (rest.length() > 4) {
									rest = rest.substring(stream.length() + 4, rest.length()); // move past  'by' and eliminate ';'
									if (rest.charAt(0) == '(') {
										rest = rest.substring(1, rest.length() - 3);
										String[] fields = rest.split(",");
										for (String field : fields) {
											field = field.trim();
											field = field.replace("'", "");
											addField(source, field, output);
										}
									} else {
										rest = rest.replace("'", "");
										addField(source, rest, output);
									}
								}
							}
							break;
							case "filter":
							{
								String fstream = rest.substring(0, rest.indexOf(" "));
								rest = rest.substring(fstream.length() + 1);
								String fsource = dataSets.get(fstream);
								//  move over the word "by "
								if (rest.length() > 3) {
									rest = rest.substring(3);
								}
								//System.out.println(rest);
								String[] phrases = rest.split(" and | or | && | \\|\\| ");
								for (String phrase : phrases) {
									String field = phrase.trim();
									if (phrase.indexOf(" ") > 0) {
										field = phrase.substring(0, phrase.indexOf(" "));
									}
									
									if (field.startsWith("date(")) {
										field = field.substring(5);
									}
									
									field = field.replace("'", "");
									field = field.replace(",", "");
									field = field.replaceAll("_Year|_Quarter|_Month|_Day$", "");
									if (fsource != null && !fsource.equals("") && !field.equals("") && !output.equals("")) {
										addField(fsource, field, output);
									}
									
									//System.out.println("SAQL");
								}
							}
							break;

							}
						}
					}
					
					System.out.println("SAQL");
				} else {
					System.out.println(type);
				}
			}
		}
		return output;
	}
	
	public void readRecipes() {
		recipes.clear();
		String nextURL = "/services/data/v58.0/wave/recipes" ;
		ThreadCluster tc = new ThreadCluster(CONCURRENCY);
		while (nextURL != null && !nextURL.equals("null")) {
			int res = agent.get(nextURL, null);
			JSONObject root = (JSONObject) agent.getResponse();
			
			for (JSONNode recipe : root.get("recipes").values()) {
				String name = recipe.get("name").asString();
				String label = recipe.get("label").asString();
				Recipe r = new Recipe(name, label);
				recipes.put(name, r);

				r.id = recipe.get("id").asString();
				r.url = recipe.get("url").asString();
				JSONNode schedule = recipe.get("scheduleAttributes");
				String frequency = schedule.get("frequency").asString();
				r.isScheduled = (frequency != null && !frequency.equals("none"));
				if (r.isScheduled) {
					r.nextSchedule = schedule.get("nextScheduledDate").asString();
				}

				tc.dispatch(new Runnable() {
					public void run() {
						int res = agent.get(r.url, null);
						JSONObject rec = (JSONObject) agent.getResponse();
						JSONNode def = rec.get("recipeDefinition");
						r.nodes = def.get("nodes").entrySet().stream().collect(Collectors.toMap((set) -> set.getKey(), (set) -> set.getValue()));
					}
				});
			}
			nextURL = root.get("nextPageUrl").asString();
		}
		tc.join();
	}
	
	public static class Instance implements Comparable<Instance> {
		public String id;
		public String dataElementId;
		public String fieldAPIName;
		public boolean isSource;
		public Source source;
		public boolean isMultiValue;
		
		public Instance(String id, String dataElementId, String fieldAPIName, boolean isSource, Source source) {
			this.dataElementId = dataElementId;
			this.fieldAPIName = fieldAPIName;
			this.isSource = isSource;
			this.source = source;
			this.id = id;
		}

		public int compare(Instance o1, Instance o2) {
			return o1.toString().compareTo(o2.toString());
		}


		@Override
		public int compareTo(Instance o) {
			return toString().compareTo(o.toString());
		}
		
	}
	
	public Map<String, Instance> getInstances(){
		if (instances.size() == 0) {
			//  Load sources
			Map<String, Source> sources = getElementSources().stream().collect(Collectors.toMap(s -> s.id, s -> s));
			//  Load all instance data
			String nextURL = "/services/data/v58.0/query?q=SELECT+Id,Data_Element__c,Field__c,Format__c,Is_Source__c,Source__c+FROM+MDR_Data_Element_Instance__c" ;
			while (nextURL != null) {
				int res = agent.get(nextURL, null);
				JSONObject root = (JSONObject) agent.getResponse();
				boolean done = root.get("done").asBoolean();
				nextURL = done ? null : root.get("nextRecordsUrl").asString();

				for (JSONNode n : root.get("records").values()) {
					Source ds = sources.get(n.get("Source__c").asString());
					Instance cf = new Instance(n.get("Id").asString(),
							n.get("Data_Element__c").asString(),
							n.get("Field__c").asString(),
							n.get("Is_Source__c").asBoolean(),
							ds);
					instances.put(cf.id, cf);
				}
			}
		}
		return instances;
	}
	
	public static class Element {
		public String id;
		public String name;
		public String parentId;
		public Element parent;
		
		public Element(String id, String name, String parentId) {
			super();
			this.id = id;
			this.name = name;
			this.parentId = parentId;
		}
		
		
	}
	
	public Map<String, Element> getElements() {
		if (elements.size() == 0) {
			String nextURL = "/services/data/v58.0/query?q=SELECT+Id,Name,Parent__c+FROM+MDR_Data_Element__c";
			int res = agent.get(nextURL, null);
			JSONObject root = (JSONObject) agent.getResponse();
			boolean done = root.get("done").asBoolean();
			nextURL = done ? null : root.get("nextRecordsUrl").asString();
			for (JSONNode n : root.get("records").values()) {
				Element e = new Element(n.get("Id").asString(),
						n.get("Name").asString(),
						n.get("Parent__c").asString());
				elements.put(e.id, e);
			}
			
			for (Element e : elements.values()) {
				if (e.parentId != null) {
					e.parent = elements.get(e.parentId);
				}
			}
		}
		return elements;
	}
	
	public void updateInstanceAPIName(Instance i, String newName) {
		JSONObject root = new JSONObject();
		String url = "/services/data/v61.0/sobjects/MDR_Data_Element_Instance__c/" + i.id;
		root.addString("Field__c", newName);
		int res = agent.patchJSON(url, root);
		JSONObject output = (JSONObject) agent.getResponse();

		System.out.println(output);
		i.fieldAPIName = newName;
	}
	
	public void deleteInstance(Instance i) {
		String url = "/services/data/v61.0/sobjects/MDR_Data_Element_Instance__c/" + i.id;
		JSONNode output = agent.delete(url);
		System.out.println(output);
		instances.remove(i.id);
	}
	
	/********************************************************
	 * DataElementSource
	 * 
	 */
	
	public static class Source {
		public String system;
		public String table;
		public String dateField;
		public String id;
		public String dateExpression;
		public String filter;
		public String idField;
		public String linkExpression;
		public String ownerField;
		public String nameFields;
		public String nameExpression;
		
		public Source(String id, String system, String table, String dateField, String dateExpression, String filter, 
				String idField, String linkExpression, String ownerField, String nameFields, String nameExpression) {
			this.system = system;
			this.table = table;
			this.dateField = dateField;
			this.id = id;
			this.dateExpression = dateExpression;
			this.filter = filter;
			this.idField = idField;
			this.linkExpression = linkExpression;
			this.ownerField = ownerField;
			this.nameFields = nameFields;
			this.nameExpression = nameExpression;
		}
	}
	
	public List<Source> getElementSources(){
		if (dataElementSources.size() == 0) {
			String nextURL = "/services/data/v58.0/query?q=SELECT+Id,Date_Field__c,System__c,Name,Date_Expression__c,Filter__c,Id_Field__c,"
					+ "Link_Expression__c,Owner_Field__c,Name_Fields__c,Name_Expression__c+FROM+MDR_Data_Element_Source__c" ;
			while (nextURL != null) {
				int res = agent.get(nextURL, null);
				JSONObject root = (JSONObject) agent.getResponse();
				boolean done = root.get("done").asBoolean();
				nextURL = done ? null : root.get("nextRecordsUrl").asString();

				for (JSONNode n : root.get("records").values()) {
					Source cf = new Source(
							n.get("Id").asString(),
							n.get("System__c").asString(),
							n.get("Name").asString(),
							n.get("Date_Field__c").asString(),
							n.get("Date_Expression__c").asString(),
							n.get("Filter__c").asString(),
							n.get("Id_Field__c").asString(),
							n.get("Link_Expression__c").asString(),
							n.get("Owner_Field__c").asString(),
							n.get("Name_Fields__c").asString(),
							n.get("Name_Expression__c").asString()
							);
					dataElementSources.add(cf);
				}
			}
		}
		return dataElementSources;
	}
	
	public Map<String, Dataflow> getDataflows() {
		if (dataflows.size() == 0) {
			String nextURL = "/services/data/v58.0/wave/dataflows" ;
			while (nextURL != null) {
				int res = agent.get(nextURL, null);
				JSONObject root = (JSONObject) agent.getResponse();
				nextURL = root.get("nextRecordsUrl") == null ? null : root.get("nextRecordsUrl").asString();
				for (JSONNode n : root.get("dataflows").values()) {
					dataflows.put(n.get("id").asString(), new Dataflow(
							n.get("id").asString(),
							n.get("name").asString(),
							n.get("label").asString()));
				}
			}
		}
		return dataflows;
	}
	
	public void saveDataflow(String id, JSONNode definition) {
		String url = "/services/data/v58.0/wave/dataflows/" + id;
		JSONObject root = new JSONObject();
		root.addTree("definition", definition);
		int res = agent.patchJSON(url, root);
		JSONObject output = (JSONObject) agent.getResponse();
		System.out.println(output.toString());
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("C:\\Users\\abeder\\eclipse-workspace\\MCIC_RDI\\test.txt"));
			out.write(output.toString());
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void runDataflow(String id) {
		JSONObject root = new JSONObject();
		root.addString("command", "Start");
		root.addString("dataflowId", id);
		String url = "/services/data/v58.0/wave/dataflowjobs";
		int res = agent.postJSON(url, root);
		JSONObject output = (JSONObject) agent.getResponse();
		System.out.println(output.toString());
	}
	
	public JSONNode executeQuery(String saql) {
		JSONObject post = new JSONObject();
		post.addString("query", saql);
		int res = agent.postJSON(EXECUTE_QUERY, post);
		JSONObject n = (JSONObject) agent.getResponse();
		if (n.getType() != JSONNode.Type.OBJECT) {
			//throw new RuntimeException("Query Failed");
			return null;
		} else {
			return n;
		}
	}
	
	public Map<String, String> getOwnerMap(){
		if (ownerMap.size() == 0) {
			String url = "/services/data/v58.0/query/?q=SELECT+Id,+Name+FROM+User";
			int res = agent.get(url, null);
			JSONObject root = (JSONObject) agent.getResponse();
			for (JSONNode r : root.get("records").values()) {
				String name = r.get("Name").asString();
				String id = r.get("Id").asString();
				ownerMap.put(id, name);
			}
		}
		return ownerMap;
	}

	public static File chooseProps(String envName, JFileChooser fc, Component comp) {
		fc.setDialogTitle("Choose properties file for " + envName);
		int res = fc.showOpenDialog(comp);
		if (res != JFileChooser.APPROVE_OPTION) {
			System.exit(0);
		}
		return fc.getSelectedFile();
	}

	public String getDatasetId(String datasetName) {
		String datasetId = null;
		String url = "/services/data/v58.0/wave/datasets?q=" + datasetName;
		int res = agent.get(url, null);
		JSONNode data = agent.getResponse();
		for (JSONNode n : data.get("datasets").values()) {
			String name = n.get("name").asString();
			if (name.equals(datasetName)) {
				datasetId = n.get("id").asString() + "/" + n.get("currentVersionId").asString();
			}
		}
		return datasetId;
	}

}
