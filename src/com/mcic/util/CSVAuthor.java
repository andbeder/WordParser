package com.mcic.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

public class CSVAuthor {
	private OutputStreamWriter out;
	private File outFile;
	private boolean isHeader;
	private Map<String, Integer> headerLookup;
	private String[] thisLine;
	private Vector<String[]> lines;
	
	public CSVAuthor(){
		outFile = null;
		out = null;
		isHeader = true;
		lines = new Vector<String[]>();
	}
	
	public void close(){
		try {
			out.flush();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		String[] header = new String[headerLookup.size()];
		for (String h : headerLookup.keySet()) {
			int i = headerLookup.get(h);
			header[i] = h;
		}
	
		for (String h : header) {
			b.append(h);
			b.append(',');
		}
		b.deleteCharAt(b.length() - 1);
		b.append("\r\n");
		for (String[] line : lines) {
			for (int i = 0;i < line.length;i++) {
				String s = line[i];
				s = (s == null) ? "" : s;
				boolean needsQuotes = s.contains(",") || s.contains("\"") || s.contains("\r") || s.contains("\n");
				if (needsQuotes) {
					//s = s.replace("\"", "\"\"");
					b.append('"');
					b.append(s);
					b.append('"');
				} else {
					b.append(s);
				}
				b.append(',');
			}
			b.deleteCharAt(b.length() - 1);
			b.append("\r\n");
		}
		return b.toString();
	}
	
	public void setHeader(String[] headerArray){
		headerLookup = new TreeMap<String, Integer>();
		thisLine = new String[headerArray.length];
		for (int i = 0;i < headerArray.length;i++){
			headerLookup.put(headerArray[i], i);
		}
		try {
			if (out != null) {
				writeNext(headerArray);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setValue(String column, String value){
		if (headerLookup == null) {
			headerLookup = new TreeMap<String, Integer>();
		}
		value = (value == null) ? "" : value;
		Integer c = headerLookup.get(column);
		if (c == null){
			c = headerLookup.size();
			headerLookup.put(column, c);
			String[] newLine = new String[c + 1];
			for (int i = 0;i < c;i++){
				newLine[i] = thisLine[i];
			}
			thisLine = newLine;
		}
		thisLine[c.intValue()] = value;
	}
	
	public void writeValues(){
		if (out == null) {
			lines.add(thisLine);
		} else {
			try {
				writeNext(thisLine);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		thisLine = new String[headerLookup.size()];
	}

	public CSVAuthor(OutputStreamWriter o){
		this();
		out = o;
	}
	
	public CSVAuthor(String path) throws IOException{
		this(new File(path));
	}
	
	public CSVAuthor(File f) throws IOException{
		this();
		outFile = f;
		out = new FileWriter(outFile);
	}
	
	public void writeNext(String[] records) throws IOException{
		boolean isFirst = true;
		for (String s : records){
			if (isFirst){
				isFirst = false;
				if (isHeader){
					isHeader = false;
				} else {
					out.write('\r');
					out.write('\n');
				}
			} else {
				out.write(',');
			}
			if (s != null){
				s = s.replace("\"", "\"\"");
				if (s.length() > 32000){
					s = s.substring(0, 32000);
				}
				boolean needsQuotes = s.contains(",") || s.contains("\"") || s.contains("\r") || s.contains("\n");
				if (needsQuotes) {
					//s = s.replace("\"", "\"\"");
					out.write('"');
					out.write(s);
					out.write('"');
				} else {
					out.write(s);
				}
			}
		}
	}
}
