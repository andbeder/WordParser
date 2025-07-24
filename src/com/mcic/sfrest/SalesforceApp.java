package com.mcic.sfrest;

import java.awt.Component;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Vector;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.filechooser.FileFilter;


public abstract class SalesforceApp {
	private SalesforceModel model;
	private SalesforceREST agent;
	private Map<String, String[]> arguments;
	private String currentDirectory;

	protected SalesforceApp(String[] switchedArgs){
		arguments = new TreeMap<String, String[]>();
		currentDirectory = null;
		Vector<String> args = new Vector<String>();
		String thisArg = "";
		File sfProps = null;
		
		for (int i = 0;i < switchedArgs.length;i++) {
			String a = switchedArgs[i];
			if (a.charAt(0) == '-') {
				if (args.size() > 0) {
					String[] values = args.toArray(new String[args.size()]);
					arguments.put(thisArg, values);
					args.clear();
				}
				thisArg = a;
			} else {
				args.add(a);
			}
		}
		if (args.size() > 0) {
			String[] values = args.toArray(new String[args.size()]);
			arguments.put(thisArg, values);
			args.clear();
		}
		
		for (String arg : arguments.keySet()) {
			String[] values = arguments.get(arg);
			switch (arg) {
			case "-d":
			case "-dir":
				currentDirectory = values[0];
				break;
			case "-sf":
				sfProps = new File(values[0]);
				if (currentDirectory != null) {
					String fileName = currentDirectory + values[0];
					sfProps = new File(fileName);
				}
			}
		}

		sfProps = (sfProps == null) ? chooseProps(currentDirectory, null) : sfProps;	
		Properties properties = new Properties();
		model = new SalesforceModel(sfProps);
		agent = new SalesforceREST(model);
	}
	
	public abstract void execute();

	public SalesforceREST getAgent() {
		return agent;
	}
	
	public String[] getArgument(String...argNames) {
		for (String arg : argNames) {
			if (arguments.containsKey(arg)) {
				return arguments.get(arg);
			}
		}
		return null;
	}

	public String getCurrentDirectory() {
		return currentDirectory;
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
