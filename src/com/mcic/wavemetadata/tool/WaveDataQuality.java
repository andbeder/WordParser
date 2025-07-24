package com.mcic.wavemetadata.tool;

import java.awt.Dialog.ModalityType;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.swing.JOptionPane;

import com.mcic.sfrest.SalesforceAgentOld;
import com.mcic.util.FuzzyScore;
import com.mcic.util.json.JSONArray;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.util.json.JSONString;
import com.mcic.wavemetadata.tool.WaveMetadata.Source;
import com.mcic.wavemetadata.tool.WaveMetadata.Dataflow;
import com.mcic.wavemetadata.tool.WaveMetadata.Dataset;
import com.mcic.wavemetadata.tool.WaveMetadata.Field;
import com.mcic.wavemetadata.tool.WaveMetadata.Instance;
import com.mcic.wavemetadata.ui.DataQualityMessage;

public class WaveDataQuality {
	SalesforceAgentOld agent;
	WaveMetadata meta;
	Map<String, Source> sourceMap;
	boolean ignoreAll;
	
	public WaveDataQuality(WaveMetadata meta) {
		this.meta = meta;
		agent = meta.getAgent();
		ignoreAll = false;
	}
	
	public JSONNode buildDataflow() {

		//  Map sources based on system and table name
		sourceMap = meta.getElementSources().stream().collect(Collectors.toMap(s -> s.system + "-" + s.table, s -> s));
		boolean isRelative = false;
		
		//  Analyze dataset field names to make sure they match
		ignoreAll = false;
		List<Instance> instanceList = meta.getInstances().values().stream().filter(i -> {
			if (i.source.system.equals("CRMA")) {
				Dataset ds = meta.getDataset(i.source.table);
				if (ds == null && !ignoreAll) {
					//JOptionPane.showMessageDialog(null, "Cannot find dataset '" + i.table + "' within CRMA for Instance " + i.id);
					DataQualityMessage msg = new DataQualityMessage(meta, i);
					int res = msg.showTableMessage();
					ignoreAll = (res == 1) ? true : ignoreAll;
					return false;
				} else {
					boolean exists = false;
					for (Field f : ds.fields.values()) {
						if (f.alias.equals(i.fieldAPIName)) {
							i.isMultiValue = f.isMultiValue;
							exists = true;							
						}
					}
					if (!exists && !ignoreAll) {
						DataQualityMessage msg = new DataQualityMessage(meta, i);
						for (Field f : ds.fields.values()) {
							msg.addMatches(f.alias);
						}
						int res = msg.showFieldMessage();
						ignoreAll = (res == 1) ? true : ignoreAll;
					}
					return exists;
				}
			} else if (i.source.system.equals("COMPASS")){
				boolean exists = false;
				Set<String> fields = agent.getFields(i.source.table);
				for (String fieldName : fields) {
					if (fieldName.equals(i.fieldAPIName)) {
						exists = true;
					}
				}
				if (!exists && !ignoreAll) {
					DataQualityMessage msg = new DataQualityMessage(meta, i);
					for (String f : fields) {
						msg.addMatches(f);
					}
					int res = msg.showFieldMessage();
					ignoreAll = (res == 1) ? true : ignoreAll;
				}
				return exists;
			} else {
				return false;
			}
		}).collect(Collectors.toList());
		
		
		//  Execute JSON append across fields and tables
		Map<String, Set<Instance>> prodFields = new TreeMap<String, Set<Instance>>();
		Map<String, Set<Instance>> datasetFields = new TreeMap<String, Set<Instance>>();
		for (Instance f : instanceList) {
			Map<String, Set<Instance>> fieldSet = f.source.system.equals("COMPASS") ? prodFields : datasetFields;
			String object = f.source.table;
			Set<Instance> cfs = fieldSet.get(object);
			if (cfs == null) {
				cfs = new TreeSet<Instance>();
				fieldSet.put(object, cfs);
			}
//			if (f.isSource) {
				cfs.add(f);
//			}
		}

		JSONObject root = new JSONObject();
		
		for (Entry<String, Set<Instance>> e : prodFields.entrySet()) {
			String objectName = e.getKey();
			Set<String> fields = new TreeSet<String>();
			for (Source s : meta.getElementSources()) {
				if (s.system.equals("COMPASS") && s.table.equals(objectName)) {
					if (s.dateField != null) {
						fields.add(s.dateField);
					}
					if (s.idField != null) {
						fields.add(s.idField);
					}
					if (s.ownerField != null) {
						fields.add(s.ownerField);
					}
					if (s.nameFields != null) {
						for (String nf : s.nameFields.split(",")) {
							fields.add(nf);
						}
					}
				}
			}
			for (Instance inst : e.getValue()) {
				fields.add(inst.fieldAPIName);
			}
			
			JSONObject node = root.addObject("loadSalesforce" + objectName);
			//node.addString("action", "digest");
			node.addString("action", "sfdcDigest");
			JSONObject p = node.addObject("parameters");
			p.addString("object", objectName);
			//p.addString("connectionName", "COMPASS_Production");
			JSONArray f = p.addArray("fields");
			for (String fName : fields) {
				f.addObject().addString("name", fName);
			}
		}
		
		for (Entry<String, Set<Instance>> e : datasetFields.entrySet()) {
			String tableName = e.getKey();
			JSONObject node = root.addObject("loadDataset" + tableName);
			node.addString("action", "edgemart");
			JSONObject p = node.addObject("parameters");
			p.addString("alias", tableName);
			
			Source ds = null;
			for (Instance i : e.getValue()) {
				ds = i.source;
			}
			if (ds != null && ds.filter != null) {
				node = root.addObject("filterDataset" + tableName);
				node.addString("action", "filter");
				p = node.addObject("parameters");
				p.addString("source", "loadDataset" + tableName);
				p.addString("saqlFilter", ds.filter);
			}
			
		}
		
		for (String objectName : prodFields.keySet()){
			if (isRelative) {
				createRelative(root, prodFields.get(objectName), "loadSalesforce" + objectName);
			} else {
				createCompute(root, prodFields.get(objectName), "loadSalesforce" + objectName);
			}
		}
		for (String datasetName : datasetFields.keySet()){
			String source = "loadDataset" + datasetName;
			Source ds = null;
			for (Instance i : datasetFields.get(datasetName)) {
				ds = i.source;
			}
			if (ds != null && ds.filter != null) {
				source = "filterDataset" + ds.table;
			}
			if (isRelative) {
				createRelative(root, datasetFields.get(datasetName), source);
			} else {
				createCompute(root, datasetFields.get(datasetName), source);
			}
		}
		
		JSONArray sources = root.addObject("appendQuality")
				.addString("action", "append")
				.addObject("parameters")
					.addBoolean("enableDisjointedSchemaMerge", true)
					.addArray("sources");
		for (String objectName : prodFields.keySet()){
			for (Instance field : prodFields.get(objectName)) {
				sources.add(new JSONString("compute" + objectName + field.fieldAPIName + "Quality"));
			}
		}		
		for (String datasetName : datasetFields.keySet()){
			for (Instance field : datasetFields.get(datasetName)) {
				if (!field.isMultiValue) {
					sources.add(new JSONString("compute" + datasetName + field.fieldAPIName + "Quality"));
				}
			}
			
		}
		
		//  Create Owner lookup using User table from Salesforce
		JSONArray flds = root.addObject("digestUser")
			.addString("action", "sfdcDigest")
			.addObject("parameters")
				.addString("object", "User")
				.addArray("fields");
		flds.addObject().addString("name", "Id");
		flds.addObject().addString("name", "Name");
		
		JSONObject p = root.addObject("joinQualityUser").addString("action", "augment").addObject("parameters");
		p.addString("operation", "LookupSingleValue")
			.addString("left", "appendQuality")
			.addString("relationship", "Owner")
			.addString("right", "digestUser");
			
		p.addArray("left_key").addString("Quality_User");
		p.addArray("right_key").addString("Id");
		p.addArray("right_select").addString("Name");
		
		p = root.addObject("computeOwner").addString("action", "computeExpression").addObject("parameters");
		p.addBoolean("mergeWithSource", true)
			.addString("source", "joinQualityUser")
			.addArray("computedFields")
				.addObject()
				.addString("type", "Text")
				.addString("name", "Quality_Owner_Name")
				.addString("label", "Quality Owner Name")
				.addString("saqlExpression", "coalesce('Owner.Name', Quality_User)");

		root.addObject("registerQuality")
			.addString("action", "sfdcRegister")
			.addObject("parameters")
				.addString("alias", "DataQuality")
				.addString("source", "computeOwner")
				.addString("name", "Data Quality");

		return root;
	}
	
	private void createRelative(JSONObject root, Set<Instance> fields, String sourceNodeName) {
		for (Instance field : fields) {
			Source ds = field.source;
			String fieldName = field.fieldAPIName;
			JSONObject obj = root.addObject("relative" + ds.table + fieldName + "Quality");
			obj.addString("action", "computeRelative");
			JSONObject p = obj.addObject("parameters");
			p.addString("source", sourceNodeName);
			p.addArray("orderBy").addObject().addString("name", ds.dateField).addString("direction", "asc");
			p.addArray("partitionBy").addString(ds.dateField);
			JSONArray a = p.addArray("computedFields");
			a.addObject()
			  .addString("name", "Numerator")
			  .addString("label", "Numerator")
			  .addObject("expression")
			    .addString("saqlExpression", "previous(Numerator) + case when '" + fieldName + "' is null then 0 else 1 end")
			    .addString("type",  "Numeric")
			    .addNumber("scale", 0)
			    .addString("default", "0");
			
			a.addObject()
			  .addString("name", "Denominator")
			  .addString("label", "Denominator")
			  .addObject("expression")
			    .addString("saqlExpression", "previous(Denominator) + 1")
			    .addString("type",  "Numeric")
			    .addNumber("scale", 0)
			    .addString("default", "0");

			a.addObject()
			  .addString("name", "Field_Name")
			  .addString("label", "Field Name")
			  .addObject("expression")
			    .addString("saqlExpression", "\\\"" + fieldName + "\\\"")
			    .addString("type",  "Text");
			    
			a.addObject()
			  .addString("name", "System_Name")
			  .addString("label", "System Name")
			  .addObject("expression")
			    .addString("saqlExpression", "\\\"" + ds.system + "\\\"")
			    .addString("type",  "Text");
			
			a.addObject()
			  .addString("name", "Table_Name")
			  .addString("label", "Table Name")
			  .addObject("expression")
			    .addString("saqlExpression", "\\\"" + ds.table + "\\\"")
			    .addString("type",  "Text");
			
			a.addObject()
			  .addString("name", "Data_Element_Source_Id")
			  .addString("label", "Data Element Source Id")
			  .addObject("expression")
			    .addString("saqlExpression", "\\\"" + ds.id + "\\\"")
			    .addString("type",  "Text");
			
			a.addObject()
			  .addString("name", "Is_Source")
			  .addString("label", "Is Source")
			  .addObject("expression")
			    .addString("saqlExpression", "\\\"" + (field.isSource ? "true" : "false") + "\\\"")
			    .addString("type",  "Text");

			Source s = sourceMap.get(ds.system + "-" + ds.table);
			if (s != null) {
				a.addObject()
				  .addString("name", "Quality_Date_epoch")
				  .addString("label", "Quality Date Epoch")
				  .addObject("expression")
				    .addString("saqlExpression", "date_to_epoch(" + s.dateExpression + ")")
				    .addString("type",  "Numeric")
				    .addNumber("scale", 0)
				    .addString("default", "0");
			}
			
			
			obj = root.addObject("compute" + ds.table + fieldName + "Quality");
			obj.addString("action", "sliceDataset");
			p = obj.addObject("parameters");
			p.addString("source", "relative" + ds.table + fieldName + "Quality");
			p.addString("mode", "select");
			JSONArray f = p.addArray("fields");
			f.addObject().addString("name", "Numerator");
			f.addObject().addString("name", "Denominator");
			f.addObject().addString("name", "Field_Name");
			f.addObject().addString("name", "System_Name");
			f.addObject().addString("name", "Table_Name");
			f.addObject().addString("name", "Quality_Date_epoch");
			f.addObject().addString("name", "Is_Source");
			f.addObject().addString("name", "Is_Source");						
			
//			a.addObject().addString("type", "Text").addString("name", "Field_Name").addString("label", "Field Name").addString("saqlExpression", "\\\"" + fieldName + "\\\"");
//			a.addObject().addString("type", "Numeric").addString("name", "Quality_Score").addString("label", "Quality Score")
//			.addString("saqlExpression", "case when '" + fieldName + "' is null then 0 else 1 end")
//			.addNumber("precision", 18).addNumber("scale", 0);
//			a.addObject().addString("type", "Text").addString("name", "System_Name").addString("label", "System Name").addString("saqlExpression", "\\\"" + ds.system + "\\\"");
//			a.addObject().addString("type", "Text").addString("name", "Table_Name").addString("label", "Table Name").addString("saqlExpression", "\\\"" + ds.table + "\\\"");
//			a.addObject().addString("type", "Text").addString("name", "Data_Element_Source_Id").addString("label", "Data Element Source Id").addString("saqlExpression", "\\\"" + ds.id + "\\\"");
//			a.addObject().addString("type", "Text").addString("name", "Is_Source").addString("label", "Is Source").addString("saqlExpression", "\\\"" + (field.isSource ? "true" : "false") + "\\\"");						
		
		}
	}	
	
	private void createCompute(JSONObject root, Set<Instance> instances, String sourceNodeName) {
		for (Instance i : instances) {
			if (!i.isMultiValue) {
				Source ds = i.source;
				String fieldName = i.fieldAPIName;
				JSONObject obj = root.addObject("compute" + ds.table + fieldName + "Quality");
				obj.addString("action", "computeExpression");
				JSONObject p = obj.addObject("parameters");
				p.addBoolean("mergeWithSource", false);
				p.addString("source", sourceNodeName);
				JSONArray a = p.addArray("computedFields");
				a.addObject()
					.addString("type", "Text")
					.addString("name", "Field_Name")
					.addString("label", "Field Name")
					.addString("saqlExpression", "\\\"" + fieldName + "\\\"");
				a.addObject()
					.addString("type", "Text")
					.addString("name", "Data_Element_Id")
					.addString("label", "Data Element Id")
					.addString("saqlExpression", "\\\"" + i.dataElementId + "\\\"");
				JSONObject qs = a.addObject()
					.addString("type", "Numeric")
					.addString("name", "Quality_Score")
					.addString("label", "Quality Score")
					.addNumber("precision", 18)
					.addNumber("scale", 0);
				if (i.isMultiValue) {
					qs.addString("saqlExpression", "case when mv_to_string('" + fieldName + "', \\\",\\\") is null then 0 else 1 end");
				} else {
					qs.addString("saqlExpression", "case when '" + fieldName + "' is null then 0 else 1 end");
				}
	
				
				a.addObject().addString("type", "Text").addString("name", "System_Name").addString("label", "System Name").addString("saqlExpression", "\\\"" + ds.system + "\\\"");
				a.addObject().addString("type", "Text").addString("name", "Table_Name").addString("label", "Table Name").addString("saqlExpression", "\\\"" + ds.table + "\\\"");
				a.addObject().addString("type", "Text").addString("name", "Data_Element_Source_Id").addString("label", "Data Element Source Id").addString("saqlExpression", "\\\"" + ds.id + "\\\"");
				a.addObject().addString("type", "Text").addString("name", "Is_Source").addString("label", "Is Source").addString("saqlExpression", "\\\"" + (i.isSource ? "true" : "false") + "\\\"");
				
				if (ds.ownerField != null) {
					a.addObject()
					  .addString("type", "Text")
					  .addString("name", "Quality_User")
					  .addString("label", "Quality User")
					  .addString("saqlExpression", "'" + ds.ownerField + "'");
				}
	
				//  Find the date field from the sources
				Source s = sourceMap.get(ds.system + "-" + ds.table);
				if (s != null && s.dateExpression != null) {
					a.addObject().addString("type", "Date").addString("name", "Quality_Date").addString("label", "Quality Date")
					.addString("saqlExpression", s.dateExpression)
					.addString("format", "yyyy-MM-dd");
	
					if (s.idField != null) {
						a.addObject()
						.addString("type", "Text")
						.addString("name", "Quality_Id")
						.addString("label", "Source Id")
						.addString("saqlExpression", "'" + s.idField + "'");
					}
					
					if (s.nameExpression != null) {
						a.addObject()
						.addString("type", "Text")
						.addString("name", "Record_Name")
						.addString("label", "Record Name")
						.addString("saqlExpression", "'" + s.nameExpression + "'");
					}
					
					if (s.linkExpression != null) {
						a.addObject()
						.addString("type", "Text")
						.addString("name", "Quality_Hyperlink")
						.addString("label", "Link")
						.addString("saqlExpression", s.linkExpression);
					}
				}
			}
		}
	}
	
	public void saveDataflow(String definition, boolean run) {
		String dataflowId = null;
		for (Dataflow d : meta.getDataflows().values()) {
			if (d.name.equals("Data_Quality")) {
				dataflowId = d.id;
			}
		}
		
		if (dataflowId != null) {
			JSONNode root = JSONNode.parse(definition);
			meta.saveDataflow(dataflowId, root);
			if (run) {
				meta.runDataflow(dataflowId);
			}
		}
	}

	public boolean isIgnoreAll() {
		return ignoreAll;
	}
}
