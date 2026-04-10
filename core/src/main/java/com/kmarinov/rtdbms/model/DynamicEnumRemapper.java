package com.kmarinov.rtdbms.model;

import java.util.HashMap;
import java.util.Map;

public class DynamicEnumRemapper implements Remapper<Integer, Number> {
	
	private Map<Number, Integer> enm = new HashMap<>();
	private Integer lastIndx = 0;

	@Override
	public Integer doRemap(Number from) {
		Integer newMapping = enm.get(from);
		if (newMapping == null) {
			enm.put(from, lastIndx);
			newMapping = lastIndx;
			lastIndx++;
		}
		
		return newMapping;
	} 
}
