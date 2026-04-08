package com.kmarinov.rtdbms.api;

import java.util.Map;

import com.kmarinov.rtdbms.model.ByteStaticRecord;

public interface Filter {
	void doFilter(ByteStaticRecord curr, ByteStaticRecord last, Map<String, Object> staticstics, int lines);

}
