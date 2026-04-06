package com.kmarinov.rtdbms.pi;

import java.io.IOException;

import com.kmarinov.rtdbms.api.AverageFilter;
import com.kmarinov.rtdbms.manager.Compressor;
import com.kmarinov.rtdbms.manager.RTManager;

public class Application {
	
	private static GPIOManager gpio_manager;
	private static RTManager rt_manager;
	
	private static final String root_dir = "./target/files";
	
	public static void main(String... args) throws IOException {
		gpio_manager = GPIOManager.instance();
		rt_manager = RTManager.getInstance(root_dir, new Compressor());
		rt_manager.addFilter(new AverageFilter());
		
		
	}

}
