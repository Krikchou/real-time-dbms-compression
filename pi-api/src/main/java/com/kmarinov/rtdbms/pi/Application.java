package com.kmarinov.rtdbms.pi;

import java.io.IOException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmarinov.rtdbms.api.AverageFilter;
import com.kmarinov.rtdbms.api.MovingWindowAverage;
import com.kmarinov.rtdbms.manager.Compressor;
import com.kmarinov.rtdbms.manager.Decryptor;
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
	
	private static final long DELTA = 1000;
	
	private static final long NS_IN_MS = 1000000;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		log.info("  _______ _                   _____           _             _____  ____  __  __  _____ \r\n"
				+ " |__   __(_)                 / ____|         (_)           |  __ \\|  _ \\|  \\/  |/ ____|\r\n"
				+ "    | |   _ _ __ ___   ___  | (___   ___ _ __ _  ___  ___  | |  | | |_) | \\  / | (___  \r\n"
				+ "    | |  | | '_ ` _ \\ / _ \\  \\___ \\ / _ \\ '__| |/ _ \\/ __| | |  | |  _ <| |\\/| |\\___ \\ \r\n"
				+ "    | |  | | | | | | |  __/  ____) |  __/ |  | |  __/\\__ \\ | |__| | |_) | |  | |____) |\r\n"
				+ "    |_|  |_|_| |_| |_|\\___| |_____/ \\___|_|  |_|\\___||___/ |_____/|____/|_|  |_|_____/ \r\n"
				+ "                                                                                       \r\n"
				+ "                                                                                       ");
		rt_manager = RTManager.getInstance(root_dir, new Compressor(), 1000);
		rt_manager.addFilter(new AverageFilter());
		rt_manager.addFilter(new MovingWindowAverage());
		if(!rt_manager.isNotEmptyFile()) {
			log.info("Init Database structure");
			rt_manager.addCol("CLK", DataType.NUM, CompressionTypeEnum.NONE, 0f);
			rt_manager.addCol("TMP", DataType.FPN, CompressionTypeEnum.DIFF, 20f);
			rt_manager.addCol("PRS", DataType.FPN, CompressionTypeEnum.DIFF, 80f);
			// sliding window averages
			rt_manager.addCol("WTM", DataType.FPN, CompressionTypeEnum.DIFF, 20f);
			rt_manager.addCol("WPR", DataType.FPN, CompressionTypeEnum.DIFF, 80f);
			//plain averages
			rt_manager.addCol("ATM", DataType.FPN, CompressionTypeEnum.DIFF, 20f);
			rt_manager.addCol("APR", DataType.FPN, CompressionTypeEnum.DIFF, 80f);
		}
		
		log.info("Init sensors");
		final Context ctx = Pi4J.newAutoContext();
		final I2CProvider p = ctx.provider("linuxfs-i2c");
		final Console console = new Console();
		
		BMP280DeviceI2C sensor = new BMP280DeviceI2C(ctx, console,p, 1, 0x76, log);
        //sensor.resetSensor();
        sensor.initSensor();
        console.println("  Dev I2C detail    " + sensor.i2cDetail());
        console.println("  Setup ----------------------------------------------------------");
        
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				log.info("Execution interrupted. Start end sequence.");
				try {
					rt_manager.close();
					Decryptor.instance(root_dir).validate();
				} catch (IOException e) {
					log.error("Could not close manager", e);
				}
				
			}
        }));
        
        log.info("Start reading sensor data");
        while(true) {
        	long startTime = System.nanoTime();
            double temp = sensor.temperatureC();
            double pres = sensor.pressurePa();
            
            rt_manager.store(ByteStaticRecord.getInstance()
            		.add("TMP", (float) temp)
            		.add("PRS", (float) pres));
            
            long endTime = System.nanoTime();
            
            if (rt_manager.isWritingToBuffer()) {
               long timeElapsed = System.nanoTime() - startTime;
               while (timeElapsed < DELTA * NS_IN_MS * 0.9) {
            	   startTime = System.nanoTime();
            	   rt_manager.encryptSingleRowSilent();
            	   timeElapsed += System.nanoTime() - startTime;
               } 
            } else {
               Thread.sleep(Duration.ofNanos((DELTA * NS_IN_MS) - (endTime - startTime)));
            }
        }
		
	}

}
