package com.mcic.util.json;

import com.mcic.util.json.JSONNode.Type;

public class JSONString extends JSONNode {
	private String value;
	
	public JSONString(String v) {
		value = v;
	}
	
	public void setValue(String v) {
		value = v;
	}
	
	

	@Override
	public Type getType() {
		return Type.STRING;
	}

	@Override
	public String toString(int l) {
		if (value == null) return "null";
		String v = value;
		//v = v.replace("\\", "\\\\");
		v = v.replace("\r", "\\r");
		v = v.replace("\n", "\\n");
		//v = v.replace("\"", "\\\"");
		v = v.replaceAll("[\\x00-\\x09\\x11\\x12\\x14-\\x1F\\x7F]", "");
		v = v.replace("&quot;", "\\\"");
		v = v.replaceAll("&#39;", "'");
		v = v.replaceAll("&gt;", ">");
		v = v.replaceAll("&lt;", "<");
		if (v.contains("{{")) {
			//System.out.println("Binding");
		} else {
			//v = v.replace("\"", "\\\"");
		}
		return "\"" + v + "\"";
	}
	
	@Override
	public String asString() {
		// TODO Auto-generated method stub
		String v = value;
		if (v.contains("{{")) {
			//System.out.println("Binding");
		}
		return value;
	}

	@Override
	public void setString(String v) {
		if (v.contains("{{")) {
			//System.out.println("Binding");
		}
		value = v;
	}
	
	
}
