package com.kmarinov.rtdbms.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.kmarinov.rtdbms.model.ByteStaticRecord;

public class FloatToIntConversionFilter implements Filter {

	private final List<String> toFilter;
	private final Integer precision;

	public FloatToIntConversionFilter(List<String> columnsToFilter, Integer precision) {
		this.toFilter = columnsToFilter;
		this.precision = precision;
	}

	@Override
	public void doFilter(ByteStaticRecord curr, ByteStaticRecord last, Map<String, Object> staticstics, int lines) {
		Map<String, Object> buff = new HashMap<>();
		for (Entry<String, Object> me : curr.getValues().entrySet()) {
			if (me.getKey() != "CLK" && me.getValue() instanceof Float fval) {
				if ((toFilter.get(0).equalsIgnoreCase("*") || toFilter.contains(me.getKey()))) {
					// can be done with bit shift operations to be faster, this is only for readability
					buff.put(me.getKey(),(int) (fval * (10^precision)));
				}

			}
		}
		
		curr.addAll(buff);

	}

}
