package com.kmarinov.rtdbms.pi;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmarinov.rtdbms.api.AverageFilter;
import com.kmarinov.rtdbms.api.MovingWindowAverage;
import com.kmarinov.rtdbms.manager.Compressor;
import com.kmarinov.rtdbms.manager.RTManager;
import com.kmarinov.rtdbms.model.ByteStaticRecord;
import com.kmarinov.rtdbms.model.CompressionTypeEnum;
import com.kmarinov.rtdbms.model.DataType;
import com.kmarinov.rtdbms.pi.devices.bmp280.BMP280DeviceI2C;
import com.pi4j.Pi4J;
import com.pi4j.context.Context;
import com.pi4j.io.i2c.I2CProvider;
import com.pi4j.util.Console;

public class Application {
	
	//private static GPIOManager gpio_manager;
	private static RTManager rt_manager;
	
	private static final String root_dir = "/home/krasi/Dev/dbfiles";
	
	private static final Logger log = LoggerFactory.getLogger(Application.class);
	
	public static void main(String[] args) throws IOException, InterruptedException {
		rt_manager = RTManager.getInstance(root_dir, new Compressor());
		rt_manager.addFilter(new AverageFilter());
		rt_manager.addFilter(new MovingWindowAverage());
		if(rt_manager.isNotEmptyFile()) {
			rt_manager.addCol("CLK", DataType.NUM, CompressionTypeEnum.NONE);
			rt_manager.addCol("TMP", DataType.FPN, CompressionTypeEnum.DIFF);
			rt_manager.addCol("PRS", DataType.FPN, CompressionTypeEnum.DIFF);
			// sliding window averages
			rt_manager.addCol("WTM", DataType.FPN, CompressionTypeEnum.DIFF);
			rt_manager.addCol("WPR", DataType.FPN, CompressionTypeEnum.DIFF);
			//plain averages
			rt_manager.addCol("ATM", DataType.FPN, CompressionTypeEnum.DIFF);
			rt_manager.addCol("APR", DataType.FPN, CompressionTypeEnum.DIFF);
		}
		
		final Context ctx = Pi4J.newAutoContext();
		final I2CProvider p = ctx.provider("linuxfs-i2c");
		final Console console = new Console();
		
		BMP280DeviceI2C sensor = new BMP280DeviceI2C(ctx, console,p, 1, 0x76, log);
        //sensor.resetSensor();
        sensor.initSensor();
        console.println("  Dev I2C detail    " + sensor.i2cDetail());
        console.println("  Setup ----------------------------------------------------------");
        
        while(true) {
            double temp = sensor.temperatureC();
            double pres = sensor.pressurePa();
            
            rt_manager.store(ByteStaticRecord.getInstance()
            		.add("TMP", (float) temp)
            		.add("PRS", (float) pres));
            
            Thread.sleep(1000);
        }
		
	}

}
