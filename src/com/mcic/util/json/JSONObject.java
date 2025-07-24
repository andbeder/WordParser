package com.mcic.util.json;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import com.mcic.util.json.JSONNode.Type;



public class JSONObject extends JSONNode{
	private TreeMap<String, JSONNode> pairs;
	
	public JSONObject() {
		pairs = new TreeMap<String, JSONNode>();
	}
	
	@Override
	public void put(String key, JSONNode value) {
		pairs.put(key, value);
	}
	
	
	
	@Override
	public Type getType() {
		// TODO Auto-generated method stub
		return Type.OBJECT;
	}

	public JSONObject addString(String key, String value) {
		pairs.put(key, new JSONString(value));
		return this;
	}
	
	public JSONObject addBoolean(String key, boolean value) {
		pairs.put(key, new JSONBoolean(value));
		return this;
	}
	
	public Set<Entry<String, JSONNode>> entrySet() {
		return pairs.entrySet();
	}
	
	public JSONObject addNumber(String key, double value) {
		pairs.put(key, new JSONNumber(value));
		return this;
	}
	
	public JSONArray addArray(String key) {
		JSONArray out = new JSONArray();
		pairs.put(key,  out);
		return out;
	}
	
	public JSONObject addObject(String key) {
		JSONObject out = new JSONObject();
		pairs.put(key,  out);
		return out;
	}
	
	@Override
	public void setObject(String key, JSONNode node) {
		// TODO Auto-generated method stub
		pairs.put(key, node);
	}

	public void addTree(String key, JSONNode val) {
		pairs.put(key, val);
	}
	
	public void clear() {
		pairs.clear();
	}

	@Override
	public Collection<JSONNode> values() {
		return pairs.values();
	}

	

	@Override
	public Collection<JSONObject> valuesAsObjects() {
		Vector<JSONObject> objs = new Vector<JSONObject>();
		for (JSONNode n : pairs.values()) {
			objs.add((JSONObject)n);
		}
		return objs;
	}

	@Override
	public boolean isCollection() {
		return true;
	}

	@Override
	public Collection<String> keySet() {
		return pairs.keySet();
	}



	@Override
	public JSONNode get(String key) {
		return pairs.get(key);
	}



	@Override
	public String toString(int l) {
		String d = "";
		for (int i = 0;i < l;i++) d += "  ";
		NavigableSet<String> v = pairs.navigableKeySet();
		String s = "";
		s += "{\r\n";
		for (String k : v) {
			JSONNode a = pairs.get(k);
			s += d + "  \"" + k + "\": " + a.toString(l + 1) + ",\r\n";
		}
		if (v.size() > 0) {
			s = s.substring(0, s.length() - 3) + "\r\n";
		}
		s += d + "}";
		return s;
	}



	@Override
	public String toCompressedString() {
		Collection<Entry<String, JSONNode>> v = pairs.entrySet();
		String s = "";
		s += "{";
		for (Map.Entry<String ,JSONNode> e : v) s += "\"" + e.getKey() + "\":" + e.getValue().toCompressedString() + ",";
		if (v.size() > 0) {
			s = s.substring(0, s.length() - 1);
		}
		s += "}";
		return s;
	}

	@Override
	public boolean contains(String s) {
		return pairs.containsKey(s);
	}
	
	
}
