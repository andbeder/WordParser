package com.mcic.util;

import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import javax.swing.filechooser.FileNameExtensionFilter;

import com.mcic.util.json.DataStream;

public class CSVParser implements DataStream {
	//private InputStream in;
	private BufferedReader reader;
	private String path;
	private int[][] stateMachine;
	private Map<String, Integer> headerLookup;
	private boolean ignoreCR;
	private String missingValue;
	String[] next, header;
	private String encoding;
	private char delimiter;
	
	private int countOf(String target, char d){
		int count = 0;
		for (int i = 0;i < target.length();i++){
			if (target.charAt(i) == d) count ++;
		}
		return count;
	}
	
	public CSVParser(File f) throws FileNotFoundException{
		FileReader r = new FileReader(f);
		char[] target = new char[255];
		encoding = "UTF-8";
		delimiter = ',';
		try {
//			r.read(target);
//			r.close();
//			if (target[0] == '�' && target[1] == '�') encoding = "UTF-16";
//			BufferedReader hin = new BufferedReader(new InputStreamReader(new FileInputStream(f), encoding));
//			//BufferedReader in = new BufferedReader(new FileReader(inFiles[i]));
//			String header = hin.readLine();
//			hin.close();
//			if (countOf(header, ',') < 4){
//				if (countOf(header, '\t') > 3) delimiter = '\t';
//			}
			
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(f), encoding));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		path = f.getPath();
		headerLookup = new TreeMap<String, Integer>();
		ignoreCR = false;
		missingValue = null;
		
		
		/*************************************************
		 * State Machine states:
		 *   0) Whitespace trap
		 *   1) Start, End of Field
		 *   2) Quote Open
		 *   3) Quote Close
		 *   4) Within Quote output
		 *   5) Outside Quote output
		 *   6) End of Line
		 *   7) End of Data stream
		 *   
		 *   Flowchart can be found in Resources: /media/CSV Reader State Machine.png
		 */
		
		stateMachine = new int[7][];
		for (int i = 0;i < stateMachine.length;i++){
			stateMachine[i] = new int[65537];
		}
		
		for (int i = 0;i < 65536;i++){
			stateMachine[0][i] = 5;
			stateMachine[1][i] = 5;
			stateMachine[2][i] = 4;
			stateMachine[3][i] = 5;
			stateMachine[4][i] = 4;
			stateMachine[5][i] = 5;
		}
		
		stateMachine[0]['"' + 1] = 2;
		stateMachine[1]['"' + 1] = 2;
		stateMachine[2]['"' + 1] = 3;
		stateMachine[3]['"' + 1] = 4;
		stateMachine[4]['"' + 1] = 3;
		stateMachine[5]['"' + 1] = 5;

		stateMachine[0][delimiter + 1] = 1;
		stateMachine[1][delimiter + 1] = 1;
		stateMachine[2][delimiter + 1] = 4;
		stateMachine[3][delimiter + 1] = 1;
		stateMachine[4][delimiter + 1] = 4;
		stateMachine[5][delimiter + 1] = 1;

		stateMachine[0]['\n' + 1] = 6;
		stateMachine[1]['\n' + 1] = 6;
		stateMachine[2]['\n' + 1] = 4;
		stateMachine[3]['\n' + 1] = 6;
		stateMachine[4]['\n' + 1] = 4;
		stateMachine[5]['\n' + 1] = 6;

		stateMachine[1]['\r' + 1] = 0;
		
		stateMachine[0][0] = 7;
		stateMachine[1][0] = 7;
		stateMachine[2][0] = 7;
		stateMachine[3][0] = 7;
		stateMachine[4][0] = 7;
		stateMachine[5][0] = 7;
	}
	
	public CSVParser(String string) throws FileNotFoundException {
		this(new File(string));
	}

	public String[] readHeader(){
		header = readNext();
		if (header != null){
			for (int i = 0;i < header.length;i++){
				headerLookup.put(header[i].toUpperCase(), i);
			}
		}
		return header;
	}
	
	
	public String[] getHeader(){
		return header;
	}
	
	public boolean hasValue(String name){
		name = name.toUpperCase();
		if (headerLookup.containsKey(name)){
			Integer i = headerLookup.get(name);
			if (i.intValue() >= next.length){
				return false;
			}
		}
		return true;
	}
	
	public String getValue(String name){
		name = name.toUpperCase();
		if (headerLookup.containsKey(name)){
			Integer i = headerLookup.get(name);
			if (i.intValue() < next.length){
				return next[i.intValue()];
			}
		}
		return missingValue;
	}
	
	public void setValue(String name, String value){
		name = name.toUpperCase();
		Integer i = headerLookup.get(name);
		if (i == null){
			i = headerLookup.size();
			headerLookup.put(name, i);
		}
		if (i.intValue() >= next.length){
			String next2[] = new String[i.intValue() + 1];
			for (int x = 0;x < next.length;x++) next2[x] = next[x];
			next2[i.intValue()] = value;
			next = next2;
		} else {
			next[i.intValue()] = value;
		}
	}
	
	public void close(){
		try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String[] readNext(){
		String[] out = null;
		StringBuilder s = new StringBuilder(256);
		try {
			Vector<String> fields = new Vector<String>();
			int state = 0;
			int c = 0;
			while (state < 6){
				c = reader.read();
				state = stateMachine[state][c + 1];
				switch (state){
				case 7:
					if (fields.size() > 0 || s.length() > 0){
						fields.add(s.toString().trim());
					}
					break;
				case 6:
				case 1:
					fields.add(s.toString().trim());
					s.delete(0, s.length());
					break;
				case 5:
				case 4:
					s.append((char)c);
				}
			}
			
			//  Line has been parsed
			if (fields.size() > 0){
				out = fields.toArray(new String[fields.size()]);
			}
		} catch (Exception e) {
		// TODO Auto-generated catch block
			e.printStackTrace();
		}
		next = out;
		return out;
	}
	
	public String getCurrent(){
		String current = "";
		for (int i = 0;i < next.length;i++){
			current += next[i];
			if (i < next.length - 1) current += ",";
		}
		return current;
	}
/*
	public static void main(String[] args){
		File f = new File("C:\\Users\\User\\Downloads\\cfs-contacts-011015 v3.csv");
		try {
			CSVParser p = new CSVParser(f);
			//CSVReader p = new CSVReader(new FileReader(f));
			int c = 0;
			String[] line = p.readNext();
			int headerLength = line.length;
			System.out.println("Header length of: " + headerLength);
			while ((line = p.readNext()) != null){
				c ++;
				if (c % 1000 == 0){
					System.out.println("Read " + c + " records");
				}
				if (line.length > headerLength){
					System.out.println("Line with length: " + line.length + " found at record: " + c);
				}
			}
			System.out.println(c + " lines read");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	*/

	public void setIgnoreCR(boolean ignoreCR) {
		this.ignoreCR = ignoreCR;
	}

	public String getPath() {
		return path;
	}

	public String getMissingValue() {
		return missingValue;
	}

	public void setMissingValue(String missingValue) {
		this.missingValue = missingValue;
	}

}
