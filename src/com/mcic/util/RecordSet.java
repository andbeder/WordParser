package com.mcic.util;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;
import java.util.Vector;

public interface RecordSet {
	public void add(String key, String value);
	public void next();
	public String toBase64();
	public long size();
}
