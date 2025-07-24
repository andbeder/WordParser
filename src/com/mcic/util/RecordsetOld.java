package com.mcic.util;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

public class RecordsetOld implements RecordSet {
	private Map<String, String[]> data;
	private int record;
	private int next;
	private int maxSize;
	static int BLOCK_SIZE = 100000;
	
	public RecordsetOld() {
		data = new LinkedHashMap<String, String[]>();
		clear();
	}
	
	public long size() {
		return record;
	}
	
	public RecordsetOld partition(int start, int count) {
		RecordsetOld out = new RecordsetOld();
		if (start + count > record) {
			count = record - start;
		}
		out.maxSize = count;
		for (String key : data.keySet()) {
			String[] vals = out.data.get(key);
			String[] source = data.get(key);
			if (vals == null) {
				vals = new String[count];
				out.data.put(key, vals);
			}
			for (int i = 0;i < count;i++) {
				vals[i] = source[start + i];
			}
		}
		out.record = count;
		return out;
	}
	
	public void add(String header, String value) {
		String[] col = data.get(header);
		if (col == null) {
			col = new String[maxSize];
			data.put(header, col);
		}
		col[record] = value;
	}
	
	public void next() {
		record ++;
		if (record >= maxSize) {
			int newSize = maxSize + BLOCK_SIZE;
			for (String h : data.keySet()) {
				String[] o = data.get(h);
				String[] n = new String[newSize];
				for (int i = 0;i < o.length;i++) {
					n[i] = o[i];
				}
				data.put(h, n);
			}
			maxSize = newSize;
		}
	}
	
	public void clear() {
		data.clear();
		record = 0;
		next = 0;
		maxSize = BLOCK_SIZE;
	}
	
	public Collection<String> getHeader(){
		return data.keySet().stream().collect(Collectors.toList());
	}
	
	public String[] getLine() {
		if (next >= record) {
			return null;
		}
		
		String[] out = new String[data.size()];
		int i = 0;
		for (String[] col : data.values()) {
			out[i++] = col[next];
		}
		next ++;
		return out;
	}
	
	
	
	@Override
	public String toString() {
    	StringBuilder b = new StringBuilder();
    	b.append(getHeader().stream().collect(Collectors.joining(",")) + "\r\n");
		String[] line = null;
		next = 0;
		while ((line = getLine()) != null) {
			for (int i = 0;i < line.length;i++) {
				String value = line[i];
				if (value != null) if (value.contains(",") || value.contains("\"")) {
					int start = 0;
					int x = 0;
					while ((x = value.indexOf("\"", start)) >= 0) {
						String left = value.substring(0, x);
						String right = value.substring(x);
						if (left.charAt(left.length() - 1) != '\\') {
							left += "\\";
						}
						value = left + right;
						start = x;
					}
					value = "\"" + value + "\"";
				}
				b.append(value);
				b.append(i == line.length - 1 ? "\r\n" : ",");
			}
		}
		return b.toString();
	}
	
	public void toGZip(GZIPOutputStream os) {
		next = 0;
		try {
			os.write((getHeader().stream().collect(Collectors.joining(",")) + "\r\n").getBytes());
			String[] line = null;
			next = 0;
			while ((line = getLine()) != null) {
				for (int i = 0;i < line.length;i++) {
					String value = line[i];
					value = value == null ? "" : value;
					if (value.matches(".+\".+")) {
						value = "\"" + value.replaceAll("\"", "\"\"") + "\"";
					}
					os.write(value.getBytes());
					os.write(i == line.length - 1 ? "\r\n".getBytes() : ",".getBytes());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void write(File f) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(f));
			out.write(toString());
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public String toBase64() {
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		try {
			GZIPOutputStream os = new GZIPOutputStream(bs);
			toGZip(os);
			os.close();
			byte[] buffer = bs.toByteArray();
			return Base64.getEncoder().encodeToString(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
