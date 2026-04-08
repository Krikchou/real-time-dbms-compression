package com.kmarinov.rtdbms.pi;

import java.util.HashMap;
import java.util.Map;

import com.pi4j.Pi4J;
import com.pi4j.context.Context;
import com.pi4j.io.spi.Spi;
import com.pi4j.io.spi.SpiConfig;
import com.pi4j.io.spi.SpiConfigBuilder;

public class GPIOManager {
	
	private Map<String, Object> registry = new HashMap<>();
	
	private final Context ctx;
	
	private GPIOManager() {
		ctx = Pi4J.newAutoContext();
	}
	
	public static GPIOManager instance() {
		return new GPIOManager();
	}
	
	
}
