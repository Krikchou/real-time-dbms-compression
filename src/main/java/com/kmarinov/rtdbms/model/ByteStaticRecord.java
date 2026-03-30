package com.kmarinov.rtdbms.model;

import java.util.HashMap;
import java.util.Map;

public class ByteStaticRecord {
	private Map<String, Object> record;
	
	public ByteStaticRecord() {
		record = new HashMap<>();
	}
	
	public static ByteStaticRecord getInstance() {
		return new ByteStaticRecord();
	}
	
	public ByteStaticRecord add(String col, Object val) {
		record.put(col, val);
		return this;
	}
	
	public Map<String, Object> getValues() {
		return record;
	}
	
	public Object find(String key) {
		return record.get(key);
	}

}
