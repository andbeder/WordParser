package com.mcic.util.json;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONNode.Type;

public class JSONArray extends JSONNode {
	private Vector<JSONNode> values;
	
	public JSONArray() {
		values = new Vector<JSONNode>();
	}
	
	@Override
	public Type getType() {
		return Type.ARRAY;
	}
	
	public Vector<JSONNode> getCollection() {
		return values;
	}	

	@Override
	public int size() {
		return values.size();
	}

	@Override
	public void add(JSONNode n) {
		values.add(n);
	}
	
	public JSONObject addObject() {
		JSONObject out = new JSONObject();
		values.add(out);
		return out;
	}
	
	public JSONNode elementAt(int i) {
		return values.elementAt(i);
	}
	
	@Override
	public void clear() {
		values.clear();
	}

	@Override
	public void addAll(Vector<JSONNode> nodes) {
		values.addAll(nodes);
	}

	
	
	@Override
	public void addAll(JSONArray node) {
		for (JSONNode n : node.values()) {
			values.add(n);
		}
	}

	@Override
	public JSONNode find(String key, String value) {
		for (JSONNode node : values) {
			JSONNode val = node.get(key);
			if (val != null) {
				if (val.asString().equals(value)){
					return node;
				}
			}
		}
		return null;
	}


	@Override
	public Collection<JSONNode> values() {
		return values;
	}

	@Override
	public boolean isCollection() {
		return true;
	}

	@Override
	public String toString(int l) {
		String d = "";
		for (int i = 0;i < l;i++) d += "  ";
		String s = "[";
		if (values.size() <= 1){
			for (JSONNode e : values) s += e.toString(l + 1) + ", ";
			if (values.size() > 0) s = s.substring(0, s.length() - 2);
			s += "]";
		} else {
			s += "\r\n";
			for (JSONNode e : values) s += d + "  " + e.toString(l + 1) + ",\r\n";
			if (values.size() > 0) s = s.substring(0, s.length() - 3) + "\r\n";
			s += d + "]";
		}
		return s;
	}
	@Override
	public void setElementAt(int i, JSONNode node) {
		values.set(i, node);
	}

	@Override
	public String toCompressedString() {
		String s = "[";
		if (values.size() <= 1){
			for (JSONNode e : values) s += e.toCompressedString() + ",";
			if (values.size() > 0) s = s.substring(0, s.length() - 1);
			s += "]";
		} else {
			for (JSONNode e : values) s += e.toCompressedString() + ",";
			if (values.size() > 0) s = s.substring(0, s.length() - 1);
			s += "]";
		}
		return s;
	}

	public JSONArray addString(String string) {
		values.add(new JSONString(string));
		return this;
	}
}
