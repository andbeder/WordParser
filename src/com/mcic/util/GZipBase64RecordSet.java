package com.mcic.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZipBase64RecordSet implements RecordSet {
	String base64;
	//ByteArrayOutputStream out;
	GZIPOutputStream gzipOS;
	Map<String, Integer> headers;
	boolean isFirstValue;
	ByteArrayOutputStream byteSteam;
	byte[] zipped;
	String encoded = null;
	StringBuilder sb;
	
	public GZipBase64RecordSet() {
		headers = new LinkedHashMap<String, Integer>();
		byteSteam = new ByteArrayOutputStream();
		gzipOS = null;
		zipped = null;
		sb = new StringBuilder();
		try {
			gzipOS = new GZIPOutputStream(byteSteam);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		isFirstValue = true;
	}

	@Override
	public void add(String key, String val) {
		StringBuilder s = sb;
		s.delete(0, s.length());
		base64 = null;
		zipped = null;
		
		if (true) {

			if (isFirstValue) {
				isFirstValue = false;
			} else {
				s.append(",");
			}
			if (val.matches("[\\s\\S]*[\"\\n][\\s\\w]*")) {
				val = val.replaceAll("\"", "\"\"");
				s.append("\"");
				s.append(val);
				s.append("\"");
			} else if (val.contains(",")) {
				s.append("\"");
				s.append(val);
				s.append("\"");
			} else {
				s.append(val);
			}
	
			try {
				gzipOS.write(s.toString().getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else {
			try {
				if (isFirstValue) {
					isFirstValue = false;
				} else {
					gzipOS.write(COMMA);
				}
				if (val.matches("[\\s\\S]*[\"\\n][\\s\\w]*")) {
					val = val.replaceAll("\"", "\"\"");
					gzipOS.write(QUOTES);
					gzipOS.write(val.getBytes());
					gzipOS.write(QUOTES);
				} else if (val.contains(",")) {
					gzipOS.write(QUOTES);
					gzipOS.write(val.getBytes());
					gzipOS.write(QUOTES);
				} else {
					gzipOS.write(val.getBytes());
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	static byte[] COMMA = ",".getBytes();
	static byte[] QUOTES = "\"".getBytes();
	
	public byte[] getZipped() {
		if (zipped == null) {
			try {
				gzipOS.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			zipped = byteSteam.toByteArray();
		}
		return zipped;
	}
	

	@Override
	public String toBase64() {
		if (encoded == null) {
			try {
				gzipOS.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			encoded = Base64.getEncoder().encodeToString(getZipped());
		}
		return encoded;
	}

	@Override
	public void next() {
		isFirstValue = true;
		try {
			gzipOS.write("\r\n".getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		try {
			return decompressGzip(getZipped());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "Error Decompressing";
	}
	
    public static String decompressGzip(byte[] gzippedData) throws IOException {
        // Create a ByteArrayInputStream from the gzipped data
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(gzippedData);
        
        // Create a GZIPInputStream to decompress the data
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
             InputStreamReader reader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(reader)) {

            // Read the decompressed data line by line and append it to a StringBuilder
            StringBuilder decompressedData = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                decompressedData.append(line).append("\n");
            }

            // Convert StringBuilder to String and remove the last newline
            return decompressedData.toString().trim();
        }
    }
    
}
