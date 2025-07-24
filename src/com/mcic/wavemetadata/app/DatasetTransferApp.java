package com.mcic.wavemetadata.app;

import java.io.File;
import java.util.Vector;
import java.util.stream.Collectors;

import javax.swing.JOptionPane;

import com.mcic.ConfiguredApp;
import com.mcic.sfrest.SalesforceAgentOld;
import com.mcic.sfrest.SalesforceModel;
import com.mcic.util.RecordsetOld;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.wavemetadata.tool.WaveMetadata;
import com.mcic.wavemetadata.tool.WaveMetadata.Dataset;
import com.mcic.wavemetadata.tool.WaveMetadata.Field;

public class DatasetTransferApp extends ConfiguredApp {
	public String datasetAPIName;
	public SalesforceAgentOld source;
	public SalesforceAgentOld target;
	public String segmentField;
	
	public static void main(String[] args) {
		DatasetTransferApp app = new DatasetTransferApp();
		//Vector<String> additionalArgs = main(args, app);
		String curDir = "";
		File sourceFile = null;
		File targetFile = null;
		String datasetName = null;
		String segmentField = null;
		
		Vector<String> additionalArgs = new Vector<String>();
		for (String arg : args) {
			additionalArgs.add(arg);
		}
		
		int i = 0;
		while (i < additionalArgs.size()) {
			String cmd = additionalArgs.elementAt(i);
			switch (cmd.toLowerCase()) {
			case "-d":
			case "-dir":
				curDir = args[++i];
				char c = curDir.charAt(curDir.length() - 1);
				if (c != '/' && c != '\\') {
					curDir += "/";
				}
				break;
			case "-source":
			case "-s":
				sourceFile = new File(curDir + args[++i]);
				break;
			case "-target":
			case "-t":
				targetFile = new File(curDir + args[++i]);
				break;
			case "-ds":
			case "-dataset":
			default:
				datasetName = args[++i];
				break;
			}
			i++;
		}
		sourceFile = (sourceFile == null) ? chooseProps(curDir, null) : sourceFile;	
		targetFile = (targetFile == null) ? chooseProps(curDir, null) : targetFile;
		if (datasetName == null) {
			datasetName = JOptionPane.showInputDialog("API Name of dataset");
		}
		if (segmentField == null) {
			segmentField = JOptionPane.showInputDialog("API Name of field to segment by");
		}

		app.source = new SalesforceAgentOld(new SalesforceModel(sourceFile));
		app.target = new SalesforceAgentOld(new SalesforceModel(targetFile));
		app.datasetAPIName = datasetName;
		app.segmentField = segmentField;
		app.init();
	}

	@Override
	public void init() {
		WaveMetadata meta = new WaveMetadata(source);
		Dataset ds = meta.getDataset(datasetAPIName);
		String dsId = meta.getDatasetId(datasetAPIName);
		
		Vector<String> segments = new Vector<String>();
		String saql = "q = load \\\"" + dsId + "\\\";"
				+ "q = group q by '" + segmentField + "';"
				+ "q = foreach q generate '" + segmentField + "' as '" + segmentField + "';";
		JSONObject post = new JSONObject();
		post.addString("query", saql);
		JSONNode data = source.postJSON("/services/data/v60.0/wave/query", post);
		for (JSONNode record : data.get("results").get("records").values()) {
			String name = record.get(segmentField).asString();
			segments.add(name);
		}
		
		RecordsetOld out = new RecordsetOld();
		
		for (String segment : segments) {
			saql = "q = load \\\"" + dsId + "\\\";"
					+ "q = filter q by '" + segmentField + "' == \\\"" + segment + "\\\";"
					+ "q = foreach q generate ";
			saql += ds.fields.values().stream().map(field -> "'" + field.name + "' as '" + field.name + "'").collect(Collectors.joining(", ")) + ";";
			post = new JSONObject();
			post.addString("query", saql);
			data = source.postJSON("/services/data/v60.0/wave/query", post);
			for (JSONNode record : data.get("results").get("records").values()) {
				for (Field f : ds.fields.values()) {
					if (record.get(f.name) != null) {
						String var = record.get(f.name).asString();
						out.add(f.name, var);
					}
				}
				out.next();
			}
		}
		
		target.writeDataset(ds.name, ds.label, ds.application, out);
	}

}
