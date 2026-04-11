package com.kmarinov.rtdbms.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
	
	public void addAll(Map<String, Object> fileds) {
		record.putAll(fileds);
	}
	
	public String dumpCSVLine(String[] dumpOrder) {
		StringBuilder sb = new StringBuilder();
		for (String s : dumpOrder) {
			sb.append(record.get(s).toString());
			sb.append(";");
		}
		
		sb.append("\r\n");
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		return record.toString();
	}
	
	@Override
	public boolean equals(Object other) {
		boolean isEqual = true;
		if(other != null && other instanceof ByteStaticRecord otherRecord) {
			for (Entry<String, Object> e : this.record.entrySet()) {
				isEqual = isEqual && e.getValue().equals(otherRecord.find(e.getKey()));
			}
			
			return isEqual;
		}
		
		return false;
	}

}
