package com.mcic;

import java.awt.Component;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.filechooser.FileFilter;

import com.mcic.sfrest.SalesforceModel;
import com.mcic.util.Progressive;

public abstract class ConfiguredApp extends Progressive {
	protected Map<String, Object> properties;
	
	public static Vector<String> main(String[] args, ConfiguredApp app) {
		String curDir = "";
		app.properties = new TreeMap<String, Object>();
		Vector<String> datasets = null;
		Vector<String> additionalArgs = new Vector<String>();
		File propFile = null;
		int i = 0;
		while (i < args.length) {
			String val = args[i];
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
			default:
				additionalArgs.add(val);
				break;
			}
			i++;
		}
		
		
		propFile = (propFile == null) ? chooseProps(curDir, null) : propFile;	
		
		
		app.properties.put("datasets", datasets);
		app.properties.put("sfConfig", propFile);
		return additionalArgs;
	}
	
	public abstract void init();

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
			res = JOptionPane.showConfirmDialog(null, "Do you want to create a config file?");
			if (res == JOptionPane.YES_OPTION) {
				fc.setDialogTitle("Choose file name to save empty properties file as");
				res = fc.showSaveDialog(comp);
				if (res == JFileChooser.APPROVE_OPTION) {
					File f = fc.getSelectedFile();
					res = JOptionPane.showConfirmDialog(comp, "Are you connecting to Production?");
					try {
						BufferedWriter r = new BufferedWriter(new FileWriter(f));
						r.write("username=\n");
						r.write("password=\n");
						r.write("securityKey=\n");
						if (res == JOptionPane.YES_OPTION) {
							r.write("endpoint=https://mcic.my.salesforce.com\n");
						} else {
							r.write("endpoint=https://mcic--crdi.sandbox.my.salesforce.com\n");
						}
						r.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			System.exit(0);
		}
		return fc.getSelectedFile();
	}
}