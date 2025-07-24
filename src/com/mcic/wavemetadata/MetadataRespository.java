package com.mcic.wavemetadata;

import java.awt.Component;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;
import java.util.stream.Collectors;

import javax.swing.JFileChooser;
import javax.swing.filechooser.FileFilter;

import com.mcic.sfrest.SalesforceModel;
import com.mcic.sfrest.SalesforceREST;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.wavemetadata.tool.WaveMetadata;
import com.mcic.wavemetadata.tool.WaveMetadata.Dataset;
import com.mcic.wavemetadata.tool.WaveMetadata.Field;

public class MetadataRespository {
	WaveMetadata meta;
	SalesforceREST agent;
	Map<String, String> elements;
	
	public static void main(String[] args) {
		String curDir = "";
		Vector<String> datasets = null;
		File propFile = null;
		int i = 0;
		while (i < args.length) {
			String cmd = args[i].toLowerCase();
			switch (cmd) {
			case "-dir":
			case "-d":
				curDir = args[++i];
				char c = curDir.charAt(curDir.length() - 1);
				if (c != '/' && c != '\\') {
					curDir += "/";
				}
				break;
			case "-sf": 
				propFile = new File(curDir + args[++i]);
				break;
			case "-datasets":
			case "-ds":
				String ds = args[++i];
				datasets = new Vector<String>();
				for (String dataset : ds.split(",")) {
					datasets.add(dataset);
				}
				break;
			}
			i++;
		}
		
		propFile = (propFile == null) ? MetadataRespository.chooseProps(curDir, null) : propFile;		
		SalesforceModel model = new SalesforceModel(propFile);
		MetadataRespository mr = new MetadataRespository(model);
		mr.run(datasets);
	}
	
	public MetadataRespository(SalesforceModel model) {
		agent = new SalesforceREST(model);
		meta = new WaveMetadata(agent);
		elements = new TreeMap<String, String>();
	}
	
	public void run(Collection<String> datasets) {
		meta.loadDatasets(datasets);
		if (datasets == null) {
			datasets = meta.getBaseDatasets().keySet();
		}
		for (String datasetName : datasets) {
			Dataset dataset = meta.getDataset(datasetName);
			for (Field field : dataset.fields.values()) {
				JSONObject o = new JSONObject();
				String elementId = getElement(field.label);
				o.addString("Element__c", elementId);
				o.addString("Table__c", datasetName);
				o.addString("Field__c", field.name);
				o.addString("System__c", "CRMA");
				int res = agent.postJSON("/services/data/v58.0/sobjects/MDR_Data_Element_Instance__c/", o);
				JSONNode n = agent.getResponse();
			}
		}
	}
	
	public String getElement(String label) {
		String id = elements.get(label);
		if (id == null) {
			JSONObject e = new JSONObject();
			e.addString("Name", label);
			int res = agent.postJSON("/services/data/v58.0/sobjects/MDR_Data_Element__c/", e);
			JSONNode n = agent.getResponse();
			id = n.get("id").asString();
			elements.put(label, id);
		}
		return id;
	}

	public static File chooseProps(String curDir, Component comp) {
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
		fc.setDialogTitle("Choose properties file for Salesforce");
		int res = fc.showOpenDialog(comp);
		if (res != JFileChooser.APPROVE_OPTION) {
			System.exit(0);
		}
		return fc.getSelectedFile();
	}
}
