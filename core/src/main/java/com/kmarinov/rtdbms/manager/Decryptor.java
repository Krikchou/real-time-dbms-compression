package com.kmarinov.rtdbms.manager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmarinov.rtdbms.model.ByteStaticRecord;
import com.kmarinov.rtdbms.model.CompressionTypeEnum;
import com.kmarinov.rtdbms.model.DataType;
import com.kmarinov.rtdbms.model.DatabaseDefinition;
import com.kmarinov.rtdbms.model.NoopRemapper;
import com.kmarinov.rtdbms.model.OffsetByRemapper;
import com.kmarinov.rtdbms.model.Remapper;
import com.kmarinov.rtdbms.model.StepRemapper;

public class Decryptor {
	private static RandomAccessFile DATA_FILE;
	private static RandomAccessFile D_DATA_FILE;
	private static RandomAccessFile DEF_FILE;
	private static RandomAccessFile DUMP_FILE;
	private DatabaseDefinition def;
	private int rec_cur;
	private int rec_ext;
	
	private static final Logger LOG = LoggerFactory.getLogger(Decryptor.class); 
	
	
	public Decryptor(String rootDir) throws IOException {
		DATA_FILE = new RandomAccessFile(rootDir + RTManager.DAF_COM_N, "rw");
		D_DATA_FILE = new RandomAccessFile(rootDir + RTManager.CMP_COM_N, "rw");
		DEF_FILE = new RandomAccessFile(rootDir + RTManager.DEF_COM_N, "rw");
		DUMP_FILE = new RandomAccessFile(rootDir + "datadump.txt","rw");
		
		this.readDefinitionFile();
	}
	
	public static Decryptor instance(String root) throws IOException {
		return new Decryptor(root);
	}
	
	private void readDefinitionFile() throws IOException {
		int cursor = 0;
		byte[] buff = new byte[4*Integer.BYTES];
		DEF_FILE.seek(cursor);
		DEF_FILE.read(buff);
		
		int columnSegmentSize = ByteBuffer.wrap(buff, 0, 4).getInt();
		LOG.info("Column segment size is {}", columnSegmentSize);
		int datatypeSegmentSize = ByteBuffer.wrap(buff, 4, 4).getInt();
		LOG.info("Datatype segment size is {}", datatypeSegmentSize);
		int compressionSegmentSize = ByteBuffer.wrap(buff, 8, 4).getInt();
		LOG.info("Compression segment size is {}", compressionSegmentSize);
		int compressionVarsSegment = ByteBuffer.wrap(buff, 12, 4).getInt();
		LOG.info("Compression variables segment size is {}", compressionSegmentSize);
		
		cursor += 3*Integer.BYTES;
		
		buff = new byte[3*Character.BYTES*columnSegmentSize];
		
		DEF_FILE.seek(cursor);
		DEF_FILE.read(buff);
		
		String[] col_names = new String[columnSegmentSize];
		
		for (int i = 0; i<columnSegmentSize; i++) {
			col_names[i] = String.valueOf(new char[] {
					ByteBuffer.wrap(buff, i * Character.BYTES * 3 , Character.BYTES).getChar(),
					ByteBuffer.wrap(buff, i * Character.BYTES * 3 + Character.BYTES , Character.BYTES).getChar(), 
					ByteBuffer.wrap(buff, i * Character.BYTES * 3 + 2* Character.BYTES, Character.BYTES).getChar()});
		}
		
		cursor+=3*Character.BYTES*columnSegmentSize;
		
		buff = new byte[Short.BYTES*datatypeSegmentSize];
		
		DataType[] datatypes = new DataType[datatypeSegmentSize];
		
		DEF_FILE.seek(cursor);
		DEF_FILE.read(buff);
		
		for (int i = 0; i<datatypeSegmentSize; i++) {
			datatypes[i] = DataType.valueOf(ByteBuffer.wrap(buff, i * Short.BYTES, Short.BYTES).getShort());
		}
		
		cursor+=Short.BYTES*datatypeSegmentSize;
		
		buff = new byte[Short.BYTES*compressionSegmentSize];
		
		CompressionTypeEnum[] compression = new CompressionTypeEnum[compressionSegmentSize];
		
		DEF_FILE.seek(cursor);
		DEF_FILE.read(buff);
		
		for (int i = 0; i<compressionSegmentSize; i++) {
			compression[i] = CompressionTypeEnum.valueOf(ByteBuffer.wrap(buff, i * Short.BYTES, Short.BYTES).getShort());
		}
		
		cursor += Short.BYTES*compressionSegmentSize;
		
		buff = new byte[Float.BYTES*compressionVarsSegment];
		
		Remapper<?,?>[] remappers = new Remapper[compressionVarsSegment]; 
		Float[] remapVals = new Float[compressionVarsSegment];
		
		DEF_FILE.seek(cursor);
		DEF_FILE.read(buff);
		
		for(int i = 0; i< compressionVarsSegment ; i++) {
			remapVals[i] = ByteBuffer.wrap(buff, i * Float.BYTES, Float.BYTES).getFloat();
			switch (compression[i]) {
			case DIFF:
				remappers[i] = new NoopRemapper();
				break;
			case ENM:
				remappers[i] = new NoopRemapper();
				break;
			case NONE:
				remappers[i] = new NoopRemapper();
				break;
			case OFFST:
				remappers[i] = new OffsetByRemapper(ByteBuffer.wrap(buff, i * Float.BYTES, Float.BYTES).getFloat());
				break;
			case STEP:
				remappers[i] = new StepRemapper(ByteBuffer.wrap(buff, i * Float.BYTES, Float.BYTES).getFloat());
				break;
			default:
				break;         
			}
		}
		
		def = new DatabaseDefinition();
		def.setColumns(col_names);
		def.setDatatypes(datatypes);
		def.setCompressionStrategies(compression);
		def.setRemapperFunctions(remappers);
		def.setRemapperVals(remapVals);
		def.setSizes(new int[]{columnSegmentSize, datatypeSegmentSize, compressionSegmentSize, compressionVarsSegment});
	}

	public void validate() throws IOException {
		LOG.info("Start data consistency validation.");
		rec_cur = 0;
		rec_ext = 0;
		DATA_FILE.seek(rec_cur);
		D_DATA_FILE.seek(rec_ext);
		byte[] recordCmp = new byte[def.getRecordSize()];
		byte[] recordExt = new byte[def.getRecordSize()];
		
		
		
		DATA_FILE.read(recordCmp);
		D_DATA_FILE.read(recordExt);
		
		rec_cur += def.getRecordSize();
		rec_ext += def.getRecordSize();
		
		if (!toData(recordCmp).equals(toData(recordExt))) {
			LOG.error("Wrong assertions");
	    }
		
		while(rec_cur < DATA_FILE.length() && rec_ext < D_DATA_FILE.length()) {
			DATA_FILE.seek(rec_cur);
			byte[] mask = new byte[Math.ceilDiv(def.getRecordSize(), 8)];
			DATA_FILE.read(mask);
			rec_cur += mask.length;
			int cmpSize = 0;
			for (int i=0;i<def.getRecordSize();i++) {
				if (((mask[i / 8] >> i % 8) & 1) == 1) {
					cmpSize ++;
				}
			}
			
			byte[] compressed = new byte[cmpSize];
			DATA_FILE.seek(rec_cur);
			DATA_FILE.read(compressed);
			rec_cur += cmpSize;
			
			cmpSize = 0;
			for (int i=0;i<def.getRecordSize();i++) {
				if (((mask[i / 8] >> i % 8) & 1) == 1) {
					recordCmp[i] = compressed[cmpSize];
					cmpSize ++;
				}
			}
			
			D_DATA_FILE.seek(rec_ext);
			D_DATA_FILE.read(recordExt);
			rec_ext += def.getRecordSize();
			
			if (!toData(recordCmp).equals(toData(recordExt))) {
					LOG.error("Wrong assertions");
			}
			
			DUMP_FILE.writeChars(toData(recordCmp).dumpCSVLine(def.getColumns()));
		}
		
		LOG.info("Data consistent after decompression with uncompressed data");
	}
	
	public ByteStaticRecord toData(byte[] array) {
		ByteStaticRecord record = ByteStaticRecord.getInstance();
		int offset = 0;
		for(int i=0;i<def.getColumns().length;i++) {
			int len = 0;
			switch(def.getDatatypes()[i]) {
			case BDC:
				len = 99;
				record.add(def.getColumns()[i], ByteBuffer.wrap(array, offset, len).getDouble());
				break;
			case FLG:
				len = Short.BYTES;
				record.add(def.getColumns()[i], def.getRemapperFunctions()[i].restore(ByteBuffer.wrap(array, offset, len).getShort()));
				break;
			case FPN:
				len = Float.BYTES;
				record.add(def.getColumns()[i], def.getRemapperFunctions()[i].restore(ByteBuffer.wrap(array, offset, len).getFloat()));
				break;
			case NUM:
				len = Integer.BYTES;
				record.add(def.getColumns()[i], def.getRemapperFunctions()[i].restore(ByteBuffer.wrap(array, offset, len).getInt()));
				break;
			case STR:
				len = Character.BYTES * 3;
				record.add(def.getColumns()[i], String.valueOf(ByteBuffer.wrap(array, offset, len).array()));
				break;
			default:
				break;
			};
		}
		
		return record;
		
	}
}
