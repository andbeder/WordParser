package com.mcic.util.json;


public class JSONBoolean extends JSONNode {
	private boolean value;
	
	public JSONBoolean(boolean v) {
		value = v;
	}
	
	public void setValue(boolean v) {
		value = v;
	}

	@Override
	public String toString(int l) {
		return (value) ? "true" : "false";
	}

	@Override
	public boolean asBoolean() {
		return value;
	}
	
}
