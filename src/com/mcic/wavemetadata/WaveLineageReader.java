package com.mcic.wavemetadata;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

import com.mcic.util.json.JSONArray;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;

public class WaveLineageReader {
	
	public Map<String, Vector<String>> datasets;
	
	public WaveLineageReader() {
		datasets = new TreeMap<String, Vector<String>>();
	}
	
	public static void main(String[] args) {
		File f = new File("C:\\Users\\abeder\\Downloads\\Compass\\retrieveUnpackaged\\wave\\AMC_KPI_Dashboard.wdash");
		WaveLineageReader reader = new WaveLineageReader();
		try {
			JSONNode o = JSONNode.parseFile(f);
			
			reader.readDashboardFields(o);
			//System.out.println(fields);
//			for (String field : fields) {
//				System.out.println(field);
//			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void addField(String dataset, String field) {
		if (!field.equals("*")) {
			Vector<String> fields = datasets.get(dataset);
			if (fields == null) {
				fields = new Vector<String>();
				datasets.put(dataset, fields);
			}
			if (!fields.contains(field)) {
				fields.add(field);
			}
		}
	}

	public void readDashboardFields(JSONNode obj) {
		
		
		for (JSONNode step : obj.get("steps").values()) {
			String type = step.get("type").asString(); 
			if (type.equals("aggregateflex") || type.equals("grain")) {
				JSONNode query = step.get("query");
				JSONNode sources = query.get("sources");
				if (sources != null) {
					for (JSONNode source : sources.values()) {
						if (source.get("name") != null) {
							String dataset = source.get("name").asString();
							for (JSONNode filterNode : source.get("filters").values()) {
								JSONNode filter = filterNode;
								String field = filter.elementAt(0).asString(); ///???????
								addField(dataset, field);						
							}
							if (source.get("groups") != null && source.get("groups").isCollection()) {
								for (JSONNode groupNode : source.get("groups").values()) {
									String field = groupNode.asString();
									addField(dataset, field);						
								}
							}
							for (JSONNode columnNode : source.get("columns").values()) {
								JSONObject column = (JSONObject)columnNode;
								JSONNode fieldNode = column.get("field");
								if (fieldNode != null) {
									String cname = fieldNode.getClass().getSimpleName();
									if (cname.equals("JSONString")) {
										String field = fieldNode.asString();
										addField(dataset, field);
									} else {
										String field = fieldNode.elementAt(1).asString();
										addField(dataset, field);
									}
								}
							}
						}
					}
				} else {
					String dataset = "";
					JSONNode ds = step.get("datasets");
					for (JSONNode n : ds.values()) {
						JSONObject o = (JSONObject)n;
						dataset = o.get("name").asString();
					}
					for (Entry<String, JSONNode> e : query.entrySet()) {
						if (e.getKey().equals("query")) {
							String queryJSON = e.getValue().asString();
							queryJSON = queryJSON.replace("\\\"", "\"");
							queryJSON = queryJSON.replace("&quot;", "\"");
							queryJSON = queryJSON.replace("&#39;", "'");
							queryJSON = queryJSON.replace("&#92;", "\\");
							JSONNode q = JSONNode.parse(queryJSON);
							if (q.get("measures") != null)
								for (JSONNode measures : q.get("measures").values()) {
									String field = measures.elementAt(1).asString();
									addField(dataset, field);						
								}
							if (q.get("values") != null)
								for (JSONNode value : q.get("values").values()) {
									String field = value.asString();
									addField(dataset, field);						
								}
							if (q.get("groups") != null) {
								for (JSONNode group : q.get("groups").values()) {
									if (group.getClass().getSimpleName().equals("JSONArray")) {
										for (JSONNode gn : group.values()) {
											String field = gn.asString();
											addField(dataset, field);
										}
									} else {
										String field = group.asString();
										addField(dataset, field);
									}
								}
							}
						}
					}
				}
			} else {
				if (!type.equals("saql")) {
					System.out.println(type);
				}
			}
		}
	}
}
