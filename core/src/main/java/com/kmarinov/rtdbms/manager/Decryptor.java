package com.kmarinov.rtdbms.manager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.kmarinov.rtdbms.model.CompressionTypeEnum;
import com.kmarinov.rtdbms.model.DataType;
import com.kmarinov.rtdbms.model.DatabaseDefinition;

public class Decryptor {
	private static RandomAccessFile DATA_FILE;
	private static RandomAccessFile D_DATA_FILE;
	private static RandomAccessFile DEF_FILE;
	private DatabaseDefinition def;
	private int rec_cur;
	private int rec_ext;
	
	
	
	public Decryptor(String rootDir) throws IOException {
		DATA_FILE = new RandomAccessFile(rootDir + RTManager.DAF_COM_N, "rw");
		D_DATA_FILE = new RandomAccessFile(rootDir + RTManager.CMP_COM_N, "rw");
		DEF_FILE = new RandomAccessFile(rootDir + RTManager.DEF_COM_N, "rw");
		
		this.readDefinitionFile();
	}
	
	private void readDefinitionFile() throws IOException {
		int cursor = 0;
		byte[] buff = new byte[3*Integer.BYTES];
		DEF_FILE.seek(cursor);
		DEF_FILE.read(buff);
		
		int columnSegmentSize = ByteBuffer.wrap(buff, 0, 4).getInt();
		System.out.println(columnSegmentSize);
		int datatypeSegmentSize = ByteBuffer.wrap(buff, 4, 4).getInt();
		System.out.println(datatypeSegmentSize);
		int compressionSegmentSize = ByteBuffer.wrap(buff, 8, 4).getInt();
		System.out.println(compressionSegmentSize);
		
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
		
		def = new DatabaseDefinition();
		def.setColumns(col_names);
		def.setDatatypes(datatypes);
		def.setCompressionStrategies(compression);
		def.setSizes(new int[]{columnSegmentSize, datatypeSegmentSize, compressionSegmentSize});
	}

	public void validate() throws IOException {
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
		
		for (int i=0;i<def.getRecordSize();i++) {
			if ((recordCmp[i] ^ recordExt[i]) != 0) {
				System.out.println("Initial Wrong assertions expected: " + String.valueOf(recordExt[i]) + " got: " + String.valueOf(recordCmp[i]));
			}
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
			
			for (int i=0;i<def.getRecordSize();i++) {
				if ((recordCmp[i] ^ recordExt[i]) != 0) {
					System.out.println("Wrong assertions expected: " + String.valueOf(recordExt[i]) + " got: " + String.valueOf(recordCmp[i]));
				}
			}
			
		}
		
		System.out.println("All done");
	}
}
