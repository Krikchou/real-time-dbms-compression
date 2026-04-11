package com.kmarinov.rtdbms.manager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmarinov.rtdbms.model.ByteStaticRecord;
import com.kmarinov.rtdbms.model.CompressionTypeEnum;
import com.kmarinov.rtdbms.model.DataType;
import com.kmarinov.rtdbms.model.DatabaseDefinition;


public class Compressor {
	
	private static final Logger LOG = LoggerFactory.getLogger(Compressor.class);
	
	public static Compressor instance() {
		return new Compressor();
	};
	
	public void doRemap(ByteStaticRecord record, DatabaseDefinition def) {
		for(int i=0;i<def.getColumns().length;i++) {
			record.add(def.getColumns()[i], def.getRemapperFunctions()[i]
					.doRemap((Number) record.find(def.getColumns()[i])));
			
		}
	}
	
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
		case BDC: 
			byte[] scale = 
					new byte[] {
				            (byte)(((BigDecimal) o).scale() >>> 24),
				            (byte)(((BigDecimal) o).scale() >>> 16),
				            (byte)(((BigDecimal) o).scale() >>> 8),
				            (byte)((BigDecimal) o).scale()};((BigDecimal) o).scale();
		    byte[] usVal = ((BigDecimal) o).unscaledValue().toByteArray();
		    try {
				ByteArrayOutputStream str = new ByteArrayOutputStream();
				str.write(scale);
				str.write(usVal);
				return str.toByteArray();
			} catch (IOException e) {
				e.printStackTrace();
			}
		    
		default:
			return null;
		}
	}

	public HuffmanStats constructHTree(Map<Byte, Integer> frequencyMap) {
		PriorityQueue<HuffmanNode> priorityQueue =
			       new PriorityQueue<>((a, b) -> a.frequency - b.frequency);

			     // Create a Huffman node for each character and add it to the priority queue
			     for (Byte c : frequencyMap.keySet()) {
			         priorityQueue.add(new HuffmanNode(c, frequencyMap.get(c)));
			     }

			     // Build the Huffman Tree
			     while (priorityQueue.size() > 1) {
			         // Remove the two nodes with the lowest frequency
			         HuffmanNode left = priorityQueue.poll();
			         HuffmanNode right = priorityQueue.poll();

			         // Create a new internal node with these two nodes
			       	// as children and add it back to the queue
			         HuffmanNode newNode =
			           new HuffmanNode(null, left.frequency + right.frequency);
			       
			         newNode.left = left;
			         newNode.right = right;
			         priorityQueue.add(newNode);
			     }

			     // The remaining node is the root of the Huffman Tree
			     HuffmanNode root = priorityQueue.poll();
			     Map<Byte, String> map = new HashMap<>();
			     this.assignCodes(root, new StringBuilder(), map);
			     return new HuffmanStats(root, map, frequencyMap);
	}
	
	private void assignCodes(HuffmanNode root, StringBuilder code, Map<Byte, String> stats) {
	     if (root == null) return;

	     // If this is a leaf node, print the character and its code
	     if (root.data != null) {
	    	 root.setCode(code.toString());
	    	 stats.put(root.data, code.toString());
	         LOG.info(String.format("%8s", Integer.toBinaryString(root.data & 0xFF)).replace(' ', '0') + ": " + code);
	     }
	     
	     // Traverse the left subtree
	     if (root.left != null) {
	         assignCodes(root.left, code.append('0'), stats);
	         code.deleteCharAt(code.length() - 1);
	     }
	     
	     // Traverse the right subtree
	     if (root.right != null) {
	         assignCodes(root.right, code.append('1'), stats);
	         code.deleteCharAt(code.length() - 1);
	     }
	}
}
