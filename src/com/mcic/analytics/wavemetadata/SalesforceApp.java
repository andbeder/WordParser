package com.mcic.analytics.wavemetadata;

import java.awt.Component;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.filechooser.FileFilter;

public abstract class SalesforceApp {
    private SalesforceREST agent;
    private SalesforceModel model;
    private Map<String, String[]> arguments;
    private String currentDirectory;
    
    protected SalesforceApp() {
        arguments = new TreeMap<>();
        currentDirectory = null;
    }
    
    protected SalesforceApp(SalesforceApp parent) {
    	this();
    	agent = parent.agent;
    	model = parent.model;
    }

    protected void setArgs(String[] switchedArgs) {
        Vector<String> args = new Vector<>();
        String thisArg = null;
        File sfProps = null;

        // Parse switches and values
        for (String a : switchedArgs) {
            if (a.startsWith("-")) {
                if (thisArg != null && !args.isEmpty()) {
                    arguments.put(thisArg, args.toArray(new String[0]));
                    args.clear();
                }
                thisArg = a;
            } else if (thisArg != null) {
                args.add(a);
            }
        }
        if (thisArg != null && !args.isEmpty()) {
            arguments.put(thisArg, args.toArray(new String[0]));
        }

        // Handle directory and props file
        for (Map.Entry<String, String[]> entry : arguments.entrySet()) {
            String arg = entry.getKey();
            String[] vals = entry.getValue();
            switch (arg) {
                case "-d":
                case "-dir":
                    currentDirectory = vals[0];
                    break;
                case "-sf":
                    sfProps = new File(
                        currentDirectory != null 
                          ? new File(currentDirectory, vals[0]).getPath()
                          : vals[0]
                    );
                    break;
            }
        }

        // Choose or load properties
        sfProps = (sfProps == null) ? chooseProps(currentDirectory, null) : sfProps;
        model = new SalesforceModel(sfProps);
        agent = new SalesforceREST(model);
    }

    public abstract void execute();

    public SalesforceREST getAgent() {
        return agent;
    }

    public String[] getArgument(String... argNames) {
        for (String name : argNames) {
            if (arguments.containsKey(name)) {
                return arguments.get(name);
            }
        }
        return null;
    }

    public String getCurrentDirectory() {
        return currentDirectory;
    }

    public static File chooseProps(String curDir, Component comp) {
        JFileChooser fc = new JFileChooser(curDir);
        fc.setFileFilter(new FileFilter() {
            public boolean accept(File f) {
                return f.isDirectory() || f.getName().endsWith(".properties");
            }
            public String getDescription() {
                return "Salesforce Properties Files (*.properties)";
            }
        });
        fc.setDialogTitle("Choose Salesforce configuration file");
        if (fc.showOpenDialog(comp) != JFileChooser.APPROVE_OPTION) {
            if (JOptionPane.showConfirmDialog(null, "Create new config file?") == JOptionPane.YES_OPTION) {
                fc.setDialogTitle("Save new Salesforce config file");
                if (fc.showSaveDialog(comp) == JFileChooser.APPROVE_OPTION) {
                    File f = fc.getSelectedFile();
                    try (BufferedWriter w = new BufferedWriter(new FileWriter(f))) {
                        w.write("username=\n");
                        w.write("password=\n");
                        w.write("securityKey=\n");
                        w.write("endpoint=\n");
                        w.write("key=\n");
                        w.write("secret=\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.exit(0);
        }
        return fc.getSelectedFile();
    }
}
