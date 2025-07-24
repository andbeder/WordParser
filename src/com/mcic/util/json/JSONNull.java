package com.mcic.util.json;

public class JSONNull extends JSONNode {

	@Override
	public String toString(int l) {
		return "null";
	}

	@Override
	public String asString() {
		return null;
	}
	
	

}
