package com.kmarinov.rtdbms.manager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import com.kmarinov.rtdbms.model.ByteStaticRecord;
import com.kmarinov.rtdbms.model.CompressionTypeEnum;
import com.kmarinov.rtdbms.model.DataType;
import com.kmarinov.rtdbms.model.DatabaseDefinition;

public class Compressor {
	
	public byte[] doCompress(ByteStaticRecord record, DatabaseDefinition def) {
		return from(record, def);
	}
	
	public byte[] merge(byte[] in, byte[] tail) throws IOException {
		int accum = 0;
		byte[] cmp = new byte[Math.ceilDiv(in.length, 8)];
		Arrays.fill(cmp, (byte) 0);
		byte[] buff = new byte[in.length];
		for (int i = 0; i<in.length;i++) {
			if ((in[i] ^ tail[i]) != 0) {
				buff[accum] = in[i];
				++accum;
				int shiftBy = (i % 8);
				cmp[i / 8] = (byte) ((cmp[i / 8] | ( 1 << shiftBy)));
			}
			
			
		}
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
		outputStream.write( cmp );
		outputStream.write( Arrays.copyOf(buff, accum) );
		
		return outputStream.toByteArray();
	}
	
	private byte[] from(ByteStaticRecord tuple, DatabaseDefinition def) {
		int cursor = 0;
		byte[] buff = new byte[def.getRecordSize()];
		for(int i=0; i < def.getColumns().length;i++) {
			Object val = tuple.find(def.getColumns()[i]);
			byte[] resBytes = applyRemap(val, def.getDatatypes()[i], def.getCompressionStrategies()[i]);
			for (int j = 0;j<resBytes.length;j++) {
				buff[j + cursor] = resBytes[j];
			}
			
			cursor += resBytes.length;
		}
		return buff;
	}
	
	private byte[] applyRemap(Object o, DataType dType, CompressionTypeEnum cType) {
		switch (dType) {
		case FLG:
		    byte[] byteArray = new byte[2];
		    byteArray[0] = (byte) (((Short) o).shortValue() >> 8);
		    byteArray[1] = (byte) ((Short) o).shortValue();
		    return byteArray;
		case FPN:
		    int intBits =  Float.floatToIntBits((Float) o);
		    return new byte[] {
		      (byte) (intBits >> 24), (byte) (intBits >> 16), (byte) (intBits >> 8), (byte) (intBits) };
		case NUM:
			int value = ((Integer) o).intValue();
		    return new byte[] {
		            (byte)(value >>> 24),
		            (byte)(value >>> 16),
		            (byte)(value >>> 8),
		            (byte)value};
		case STR:
			return ((String) o).getBytes();
		default:
			return null;
		}
	}

}
