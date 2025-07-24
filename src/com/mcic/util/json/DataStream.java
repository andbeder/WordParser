package com.mcic.util.json;

public interface DataStream {
	public String[] readNext();
	public String[] getHeader();
	public String getValue(String key);
	public boolean hasValue(String key);
}
