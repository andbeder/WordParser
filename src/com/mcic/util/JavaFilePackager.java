package com.mcic.util;

import javax.swing.JFileChooser;
import javax.swing.UIManager;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class JavaFilePackager {

    public static void main(String[] args) {
        // Set system look and feel for better integration with the OS
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception e) {
            // Fall back to default look and feel
            e.printStackTrace();
        }
        
        // Open dialog to select one or more files and/or directories
        JFileChooser openChooser = new JFileChooser();
        openChooser.setDialogTitle("Select Files and/or Folders");
        openChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
        openChooser.setMultiSelectionEnabled(true);
        
        int openResult = openChooser.showOpenDialog(null);
        if (openResult != JFileChooser.APPROVE_OPTION) {
            System.out.println("No files or folders selected.");
            System.exit(0);
        }
        File[] selectedFiles = openChooser.getSelectedFiles();
        
        // Collect all .java files from the selected files/folders
        List<File> javaFiles = new ArrayList<>();
        for (File file : selectedFiles) {
            if (!file.exists()) {
                System.out.println("Path does not exist: " + file.getAbsolutePath());
                continue;
            }
            if (file.isFile()) {
                if (file.getName().endsWith(".java") || file.getName().endsWith(".js") 
                		|| file.getName().endsWith(".html") || file.getName().endsWith(".css")) {
                    javaFiles.add(file);
                }
            } else if (file.isDirectory()) {
                gatherJavaFiles(file, javaFiles);
            }
        }
        
        // Open dialog to select where to save the packaged .txt file
        JFileChooser saveChooser = new JFileChooser();
        saveChooser.setDialogTitle("Save Packaged File");
        saveChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
        int saveResult = saveChooser.showSaveDialog(null);
        if (saveResult != JFileChooser.APPROVE_OPTION) {
            System.out.println("No output file selected.");
            System.exit(0);
        }
        File outputFile = saveChooser.getSelectedFile();
        
        // Write the gathered Java files to the chosen output file in Markdown format
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
            for (File javaFile : javaFiles) {
                writer.println("**File: " + javaFile.getAbsolutePath() + "**");
                String extension = javaFile.getName();
                extension = extension.substring(extension.lastIndexOf('.') + 1);
                writer.println("```" + extension);
                
                // Read the file content and write it to the output
                List<String> lines = Files.readAllLines(javaFile.toPath(), StandardCharsets.UTF_8);
                for (String line : lines) {
                    writer.println(line);
                }
                writer.println("```");  // End of code block
                writer.println();       // Blank line between files
            }
            System.out.println("Packaged " + javaFiles.size() + " .java file(s) into " + outputFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Recursively traverses directories to gather all .java files.
     * 
     * @param dir The directory to search.
     * @param javaFiles The list that accumulates found Java files.
     */
    private static void gatherJavaFiles(File dir, List<File> javaFiles) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    gatherJavaFiles(file, javaFiles);
                } else if (file.isFile()) {
                    if (file.getName().endsWith(".java") || file.getName().endsWith(".js") 
                    		|| file.getName().endsWith(".html") || file.getName().endsWith(".css")) {
                    	javaFiles.add(file);
                    }
                }
            }
        }
    }
}
