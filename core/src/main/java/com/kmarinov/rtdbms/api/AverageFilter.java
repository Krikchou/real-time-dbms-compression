package com.kmarinov.rtdbms.api;

import java.util.Map;
import java.util.Map.Entry;

import com.kmarinov.rtdbms.model.ByteStaticRecord;

public class AverageFilter implements Filter {
	
	private static final String PREFX = "AVRG_";

	@Override
	public void doFilter(ByteStaticRecord curr, ByteStaticRecord last, Map<String, Object> statistics, int lines) {
		for (Entry<String, Object> me : curr.getValues().entrySet()) {
			if (me.getKey()!= "CLK" && me.getValue() instanceof Number currVal) {
				float result = switch (currVal) {
				case Integer i -> ((((Float) statistics.getOrDefault(PREFX + me.getKey(), 0f)).floatValue() * lines) + i.floatValue()) / (float) (lines + 1);
				case Float f -> ((((Float) statistics.getOrDefault(PREFX + me.getKey(), 0f)).floatValue() * lines) + f.floatValue()) / (float) (lines + 1);
				case Short s -> ((((Float) statistics.getOrDefault(PREFX + me.getKey(), 0f)).floatValue() * lines) + s.floatValue()) / (float) (lines + 1);
				default -> 0f;
				};
				
				statistics.put(PREFX + me.getKey(), result);
				curr.add("A" + me.getKey().substring(0, 2), result);
			}
		}
	}
}
