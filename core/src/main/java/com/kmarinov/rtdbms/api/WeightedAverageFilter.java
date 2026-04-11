package com.kmarinov.rtdbms.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.kmarinov.rtdbms.model.ByteStaticRecord;

public class WeightedAverageFilter implements Filter {
	
	private static final String PREFX = "QSUM_";
	private final Float sigma;
	
	public WeightedAverageFilter(Float sigma) {
		this.sigma = sigma;
	}

	@Override
	public void doFilter(ByteStaticRecord curr, ByteStaticRecord last, Map<String, Object> statistics, int lines) {
		Map<String, Object> temp = new HashMap<>();
		for (Entry<String, Object> me : curr.getValues().entrySet()) {
			if (me.getKey()!= "CLK" && me.getValue() instanceof Number currVal) {
				float result = switch (currVal) {
				case Integer i -> ((((Float) statistics.getOrDefault(PREFX + me.getKey(), 0f)).floatValue() * (1-sigma)) + i.floatValue() * sigma);
				case Float f -> ((((Float) statistics.getOrDefault(PREFX + me.getKey(), 0f)).floatValue() * (1-sigma)) + f.floatValue() * sigma);
				case Short s -> ((((Float) statistics.getOrDefault(PREFX + me.getKey(), 0f)).floatValue() * (1-sigma)) + s.floatValue() * sigma);
				default -> 0f;
				};
				
				statistics.put(PREFX + me.getKey(), result);
				temp.put("Q" + me.getKey().substring(0, 2), result);
			}
		}
		
		curr.addAll(temp);
	}
}
