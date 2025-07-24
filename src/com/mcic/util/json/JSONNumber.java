package com.mcic.util.json;

public class JSONNumber extends JSONNode {
	private double value;
	
	public JSONNumber(double n) {
		value = n;
	}
	
	public void setValue(double n) {
		value = n;
	}
	
	
	
	@Override
	public String asString() {
		return Double.toString(value);
	}

	@Override
	public String toString(int l) {
		if (value == (double)(int)value)
			return Integer.toString((int)value);
		return Double.toString(value);
	}


	@Override
	public double asDouble() {
		return value;
	}

	@Override
	public int asInt() {
		return (int)value;
	}

	@Override
	public void setInt(int i) {
		value = i;
	}
	
	
}
