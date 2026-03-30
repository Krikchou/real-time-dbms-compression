package com.kmarinov.rtdbms;

import java.io.IOException;

import com.kmarinov.rtdbms.api.AverageFilter;
import com.kmarinov.rtdbms.manager.Compressor;
import com.kmarinov.rtdbms.manager.Decryptor;
import com.kmarinov.rtdbms.manager.RTManager;
import com.kmarinov.rtdbms.model.ByteStaticRecord;
import com.kmarinov.rtdbms.model.CompressionTypeEnum;
import com.kmarinov.rtdbms.model.DataType;

public class Application {
	
	private static RTManager manager;
	
	public static void main(String... args) throws IOException {
		manager = RTManager.getInstance("D:\\test_db", new Compressor());
		manager.addFilter(new AverageFilter());
		
		System.out.println("ManagerInstantiated");
		
		if (!manager.isNotEmptyFile()) {
		   manager.addCol("CLK", DataType.NUM, CompressionTypeEnum.NONE);
		   manager.addCol("CL1", DataType.FLG, CompressionTypeEnum.DIFF);
		   manager.addCol("CL2", DataType.FPN, CompressionTypeEnum.ENM);
		   manager.addCol("CL3", DataType.NUM, CompressionTypeEnum.NONE);
		}
		
		for (int i = 0; i< 50000; i++) {
			Short shrt = (Integer.valueOf(i / 100)).shortValue();
			Float flt = Float.valueOf(i / 23);
			
			manager.store(ByteStaticRecord.getInstance()
					.add("CL1", shrt )
					.add("CL2", flt )
					.add("CL3", i ));
		}
		
		manager.close();
		
		Decryptor d = new Decryptor("D:\\test_db");
		d.validate();
	}

}
