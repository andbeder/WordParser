package com.mcic.wavemetadata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Vector;

import com.mcic.ConfiguredApp;
import com.mcic.sfrest.SalesforceModel;
import com.mcic.sfrest.SalesforceREST;
import com.mcic.util.json.JSONArray;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.wavemetadata.tool.WaveMetadata;
import com.mcic.wavemetadata.tool.WaveMetadata.Dataset;
import com.mcic.wavemetadata.tool.WaveMetadata.Field;

public class UnpivotApp extends ConfiguredApp {
	SalesforceREST agent;
	String datasetName;

	public static void main(String[] args) {
		UnpivotApp app = new UnpivotApp();
		Vector<String> additionalArgs = main(args, app);
		Vector<String> fields = new Vector<String>();
		int i = 0;

		while (i < additionalArgs.size()) {
			String cmd = additionalArgs.elementAt(i);
			switch (cmd.toLowerCase()) {
			case "-t":
			case "-dataset":
				app.datasetName = additionalArgs.elementAt(++i);
			default:
				break;
			}
			i++;
		}

//		if (fields.size() == 0) {
//			System.out.println("Use the following command line arguments:\n"
//					+ "  -t -dataset  <API name of dataset>\n"
//					+ "  -d -dir  <name of directory containing login parameters>\n" + "  -id <PSLP Case Id>"
//					+ "  -sf <name of file containing login parameters>");
//		} else {
//			app.init();
//		}
		app.init();
	}
	
	@Override
	public void init() {
		try {
			File propFile = (File) properties.get("sfConfig");
			SalesforceModel model = new SalesforceModel(propFile);
			agent = new SalesforceREST(model);
			WaveMetadata meta = new WaveMetadata(agent);
			
			BufferedWriter r = new BufferedWriter(new FileWriter("Input Variables.csv"));
			r.write("Variable Name,Variable Label,Dataset\n");
			
			Dataset ds = meta.getDataset("Report_PSLP_with_Layers");
			JSONObject dataflow = (JSONObject) JSONNode.parse(dataflowJSON);
			JSONObject append = (JSONObject) dataflow.get("append_All");
			JSONArray sources = ((JSONObject)append.get("parameters")).addArray("sources");
			
			for (Field field : ds.fields.values()) {
				String nodeName = null;
				if (!field.isMultiValue) {
					r.write(field.name + "," + field.label + "," + ds.name + "\n");
				}
				if (field.type == WaveMetadata.Type.Dimension && !field.isMultiValue) {
					nodeName = "Text_" + field.name;
				} else if (field.type == WaveMetadata.Type.Measure) {
					nodeName = "Number_" + field.name;
				} else if (field.type == WaveMetadata.Type.Date ){
					nodeName = "Date_" + field.name;
				}
				
				if (nodeName != null) {
					
					JSONObject node = dataflow.addObject(nodeName);
					node.addString("action", "computeExpression");
					JSONObject p = node.addObject("parameters");
					p.addBoolean("mergeWithSource", false);
					p.addString("source", "filter_LatestParent");
					JSONArray cf = p.addArray("computedFields");
					cf.addObject()
					  .addString("type", "Text")
					  .addString("name", "Record_Id")
					  .addString("label", "Record Id")
					  .addString("saqlExpression", "Claim__c");
					cf.addObject()
					  .addString("type", "Text")
					  .addString("name", "Input_Variable")
					  .addString("label", "Input Variable")
					  .addString("saqlExpression", "\\\"" + field.name + "\\\"");

					if (field.type == WaveMetadata.Type.Dimension) {			
						cf.addObject()
						  .addString("type", "Text")
						  .addString("name", "Input_Category")
						  .addString("label", "Input Category")
						  .addString("saqlExpression", "case when '" + field.name + "' is null then \\\"(none)\\\" else '" + field.name + "' end");
						cf.addObject()
						  .addString("type", "Text")
						  .addString("name", "Input_Type")
						  .addString("label", "Input Type")
						  .addString("saqlExpression", "\\\"Categorical\\\"");
					} else if (field.type == WaveMetadata.Type.Measure) {
						cf.addObject()
						  .addString("type", "Numeric")
						  .addString("name", "Input_Value")
						  .addString("label", "Input Value")
						  .addString("saqlExpression", "'" + field.name + "'")
						  .addNumber("precision", 18)
						  .addNumber("scale", 2);
						cf.addObject()
						  .addString("type", "Text")
						  .addString("name", "Input_Type")
						  .addString("label", "Input Type")
						  .addString("saqlExpression", "\\\"Number\\\"");
					} else if (field.type == WaveMetadata.Type.Date) {
						cf.addObject()
						  .addString("type", "Numeric")
						  .addString("name", "Input_Value")
						  .addString("label", "Input Value")
						  .addString("saqlExpression", "'" + field.name + "_sec_epoch'")
						  .addNumber("precision", 18)
						  .addNumber("scale", 2);
						cf.addObject()
						  .addString("type", "Text")
						  .addString("name", "Input_Type")
						  .addString("label", "Input Type")
						  .addString("saqlExpression", "\\\"Date\\\"");
					}
					sources.addString(nodeName);
				}
			}
			r.close();

			try {
				BufferedWriter out = new BufferedWriter(new FileWriter("out.json"));
				out.write(dataflow.toString());
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public final String dataflowJSON = "{\r\n"
			+ "  \"Report_PSLP_with_Layers\": {\r\n"
			+ "    \"action\": \"edgemart\",\r\n"
			+ "    \"parameters\": {\r\n"
			+ "      \"alias\": \"Report_PSLP_with_Layers\"\r\n"
			+ "    }\r\n"
			+ "  },\r\n"
			+ "  \"filter_LatestParent\": {\r\n"
			+ "    \"action\": \"filter\",\r\n"
			+ "    \"parameters\": {\r\n"
			+ "      \"source\": \"Report_PSLP_with_Layers\",\r\n"
			+ "      \"saqlFilter\": \"'Snapshot_Date_Key' == \\\"20241231\\\" && 'Baby_Flag' == \\\"No\\\" && 'Retained' == \\\"True\\\"\"\r\n"
			+ "    }\r\n"
			+ "  },\r\n"
			+ "  \"append_All\": {\r\n"
			+ "    \"action\": \"append\",\r\n"
			+ "    \"parameters\": {\r\n"
			+ "      \"enableDisjointedSchemaMerge\": true,\r\n"
			+ "      \"sources\": [\r\n"
			+ "        \"Text_Involved_Departments_Primary__c\",\r\n"
			+ "        \"Text_Patient_Age_bucket\"\r\n"
			+ "      ]\r\n"
			+ "    }\r\n"
			+ "  },\r\n"
//			+ "  \"mark_rownum\": {\r\n"
//			+ "    \"action\": \"computeRelative\",\r\n"
//			+ "    \"parameters\": {\r\n"
//			+ "      \"source\": \"filter_LatestParent\",\r\n"
//			+ "      \"orderBy\": [\r\n"
//			+ "        {\r\n"
//			+ "          \"name\": \"Snapshot_Date_Key\",\r\n"
//			+ "          \"direction\": \"asc\"\r\n"
//			+ "        }\r\n"
//			+ "      ],\r\n"
//			+ "      \"partitionBy\": [\r\n"
//			+ "        \"Claim__c\"\r\n"
//			+ "      ],\r\n"
//			+ "      \"computedFields\": [\r\n"
//			+ "        {\r\n"
//			+ "          \"name\": \"rownum\",\r\n"
//			+ "          \"label\": \"rownum\",\r\n"
//			+ "          \"expression\": {\r\n"
//			+ "            \"saqlExpression\": \"previous(rownum) + 1\",\r\n"
//			+ "            \"type\": \"Numeric\",\r\n"
//			+ "            \"scale\": 0,\r\n"
//			+ "            \"default\": \"0\"\r\n"
//			+ "          }\r\n"
//			+ "        }\r\n"
//			+ "      ]\r\n"
//			+ "    }\r\n"
//			+ "  },\r\n"
//			+ "  \"filter_RowOne\": {\r\n"
//			+ "    \"action\": \"filter\",\r\n"
//			+ "    \"parameters\": {\r\n"
//			+ "      \"source\": \"mark_rownum\",\r\n"
//			+ "      \"saqlFilter\": \"rownum == 1\"\r\n"
//			+ "    }\r\n"
//			+ "  },"
			+ "  \"Regression Unpivoted\": {\r\n"
			+ "    \"action\": \"sfdcRegister\",\r\n"
			+ "    \"parameters\": {\r\n"
			+ "      \"source\": \"append_All\",\r\n"
			+ "      \"alias\": \"Regression_Unpivoted\",\r\n"
			+ "      \"name\": \"Regression Unpivoted\"\r\n"
			+ "    }\r\n"
			+ "  }\r\n"
			+ "}";

}
