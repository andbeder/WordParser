package com.mcic.wavemetadata;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.stream.Collectors;

import com.mcic.util.json.JSONNode;
import com.mcic.util.json.Pair;
import com.mcic.wavemetadata.tool.WaveMetadata.Dataset;
import com.mcic.wavemetadata.tool.WaveMetadata.Field;
import com.mcic.wavemetadata.tool.WaveMetadata.Type;

public class ResultsTester {
	public static class ResultField {
		public Type type;
		public String name;
		public String label;
		public String alias;

		private ResultField(Type type, String name, String label, String alias) {
			this.type = type;
			this.name = name;
			this.label = label;
			this.alias = (alias == null) ? name : alias;
		}
	}

	
	public static boolean doTest(JSONNode prodData, JSONNode devData, String datasetName) {
		boolean success = true;
		DecimalFormat df = new DecimalFormat("#,###.#");
		
		JSONNode results1 = prodData.get("results");
		JSONNode records1 = results1.get("records");
		JSONNode records2 = devData.get("results").get("records");
		JSONNode proj = results1.get("metadata").elementAt(0).get("lineage").get("projections");
		
		Vector<ResultField> dimList = new Vector<ResultField>();
		Vector<ResultField> meaList = new Vector<ResultField>();
		for (JSONNode n : proj.values()) {
			String alias = n.get("field").get("id").asString().substring(2);
			Type type = (n.get("field").get("type").asString().equals("numeric")) ? Type.Measure : Type.Dimension;
			String name = (n.get("inputs") == null) ? alias : n.get("inputs").elementAt(0).get("id").asString().substring(2);
			alias = (alias == null) ? name : alias;
			if (type == Type.Dimension) {
				dimList.add(new ResultField(type, name, name, alias));
			} else {
				meaList.add(new ResultField(type, name, name, alias));
			}
		}

		int i = 0;
		int j = 0;
		
		while (i < records1.size() && j < records2.size()) {
			JSONNode r1 = records1.elementAt(i);
			JSONNode r2 = records2.elementAt(j);
			
			Vector<Pair<String, String>> pair1 = new Vector<Pair<String, String>>();
			Vector<Pair<String, String>> pair2 = new Vector<Pair<String, String>>();
			for (ResultField f : dimList) {
				pair1.add(new Pair<String, String>(f.name, r1.get(f.name).asString()));
				pair2.add(new Pair<String, String>(f.name, r2.get(f.name).asString()));
			}
			String dim1 = pair1.stream().map(pair -> pair.first + ": " + pair.second).collect(Collectors.joining(", "));
			String dim2 = pair2.stream().map(pair -> pair.first + ": " + pair.second).collect(Collectors.joining(", "));
			
			int comp = dim1.compareTo(dim2);
			if (comp == 0) {
				for (ResultField m : meaList) {
					double value = r1.get(m.name).asDouble();
					double check = r2.get(m.name).asDouble();
					if (value != check) {
						System.out.println("Values don't match in " + datasetName + " for (" + dim1 + ") " + m.name + ": " + df.format(value) + " (prod) " + df.format(check) + " (dev)");
						success = false;
					}
				}
				i++;
				j++;
			} else if (comp < 0) {
				System.out.println("Record missing from " + datasetName + " in Dev (" + dim2 + ")");
				success = false;
				i ++;
			} else {
				System.out.println("Record missing from " + datasetName + " in Prod (" + dim1 + ")");
				success = false;
				j ++;				
			}
		}
		
		return success;
	}
}
