package com.kmarinov.rtdbms.manager;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmarinov.rtdbms.api.Filter;
import com.kmarinov.rtdbms.model.ByteStaticRecord;
import com.kmarinov.rtdbms.model.CompressionTypeEnum;
import com.kmarinov.rtdbms.model.DataType;
import com.kmarinov.rtdbms.model.DatabaseDefinition;
import com.kmarinov.rtdbms.model.NoopRemapper;
import com.kmarinov.rtdbms.model.OffsetByRemapper;
import com.kmarinov.rtdbms.model.Remapper;
import com.kmarinov.rtdbms.model.StepRemapper;

public class RTManager implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(RTManager.class);

	public static final String DAF_COM_N = "/datafile";
	public static final String DEF_COM_N = "/definition";
	public static final String LOG_COM_N = "/log";
	public static final String STF_COM_N = "/stat";
	public static final String CMP_COM_N = "/def_datafile";
	public static final String HCF_COM_N = "/huff_buff_file";
	public static final String HSF_COM_N = "/huff_map_file";

	private static RandomAccessFile DATA_FILE;
	private static RandomAccessFile D_DATA_FILE;
	private static RandomAccessFile DEF_FILE;
	private static RandomAccessFile LOG_FILE;
	private static RandomAccessFile STAT_FILE;
	private static RandomAccessFile HBUF_FILE;
	private static RandomAccessFile HMAP_FILE;
	private final Compressor compressor;
	private final int dataLineCutoff;
	private DatabaseDefinition def;
	private int data_lines;
	private int data_cursor;
	private int compare_data_cursor;
	private int last_huffman_byte = 0;
	private int huff_curr = 0;
	private int first_huffman_byte = 0;
	private long huff_last_line = 0;
	private ByteStaticRecord uncompressedLast;
	private List<Filter> filterChain = new ArrayList<>();
	private Map<String, Object> stats = new ConcurrentHashMap<>();
	private Map<Byte, Integer> frequencies = new ConcurrentHashMap<>();
	private HuffmanStats huffTree;
	private byte[] uncompressedLastBytes = new byte[0];
	private boolean isNotEmptyFile = false;
	private boolean isWritingToBuffer = false;
	private boolean isBufferingData = false;

	private RTManager(String rootDir, Compressor compressor, int datalineCutoff) throws IOException {
		this.compressor = compressor;
		this.dataLineCutoff = datalineCutoff;
		// step load files:
		DATA_FILE = new RandomAccessFile(rootDir + DAF_COM_N, "rw");
		D_DATA_FILE = new RandomAccessFile(rootDir + CMP_COM_N, "rw");
		DEF_FILE = new RandomAccessFile(rootDir + DEF_COM_N, "rw");
		LOG_FILE = new RandomAccessFile(rootDir + LOG_COM_N, "rw");
		STAT_FILE = new RandomAccessFile(rootDir + STF_COM_N, "rw");
		HBUF_FILE = new RandomAccessFile(rootDir + HCF_COM_N, "rw");
		HMAP_FILE = new RandomAccessFile(rootDir + HSF_COM_N, "rw");

		// step read definition:
		readDefinitionFile();
		// step read stat file
		readStatFile();
	}

	public static RTManager getInstance(String rootDir, Compressor compressor, int datalineCutoff) throws IOException {
		return new RTManager(rootDir, compressor, datalineCutoff);
	}

	private void readDefinitionFile() throws IOException {
		int cursor = 0;
		byte[] buff = new byte[4 * Integer.BYTES];
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

		cursor += 4 * Integer.BYTES;

		buff = new byte[3 * Character.BYTES * columnSegmentSize];

		DEF_FILE.seek(cursor);
		DEF_FILE.read(buff);

		String[] col_names = new String[columnSegmentSize];

		for (int i = 0; i < columnSegmentSize; i++) {
			col_names[i] = String.valueOf(new char[] {
					ByteBuffer.wrap(buff, i * Character.BYTES * 3, Character.BYTES).getChar(),
					ByteBuffer.wrap(buff, i * Character.BYTES * 3 + Character.BYTES, Character.BYTES).getChar(),
					ByteBuffer.wrap(buff, i * Character.BYTES * 3 + 2 * Character.BYTES, Character.BYTES).getChar() });
		}

		cursor += 3 * Character.BYTES * columnSegmentSize;

		buff = new byte[Short.BYTES * datatypeSegmentSize];

		DataType[] datatypes = new DataType[datatypeSegmentSize];

		DEF_FILE.seek(cursor);
		DEF_FILE.read(buff);

		for (int i = 0; i < datatypeSegmentSize; i++) {
			datatypes[i] = DataType.valueOf(ByteBuffer.wrap(buff, i * Short.BYTES, Short.BYTES).getShort());
		}

		cursor += Short.BYTES * datatypeSegmentSize;

		buff = new byte[Short.BYTES * compressionSegmentSize];

		CompressionTypeEnum[] compression = new CompressionTypeEnum[compressionSegmentSize];

		DEF_FILE.seek(cursor);
		DEF_FILE.read(buff);

		for (int i = 0; i < compressionSegmentSize; i++) {
			compression[i] = CompressionTypeEnum
					.valueOf(ByteBuffer.wrap(buff, i * Short.BYTES, Short.BYTES).getShort());
		}

		cursor += Short.BYTES * compressionSegmentSize;

		buff = new byte[Float.BYTES * compressionVarsSegment];

		Remapper<?, ?>[] remappers = new Remapper[compressionVarsSegment];
		Float[] remapVals = new Float[compressionVarsSegment];

		DEF_FILE.seek(cursor);
		DEF_FILE.read(buff);

		for (int i = 0; i < compressionVarsSegment; i++) {
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
		def.setSizes(
				new int[] { columnSegmentSize, datatypeSegmentSize, compressionSegmentSize, compressionVarsSegment });
	}

	private void readStatFile() throws IOException {
		int cursor = 0;
		STAT_FILE.seek(cursor);
		byte[] buff = new byte[2 * Integer.BYTES + 1];
		STAT_FILE.read(buff);
		data_lines = ByteBuffer.wrap(buff, 0, 4).getInt();
		data_cursor = ByteBuffer.wrap(buff, 4, 4).getInt();
		isNotEmptyFile = ByteBuffer.wrap(buff, 8, 1).get() == (byte) 1;
	}

	public int addCol(String name, DataType type, CompressionTypeEnum ctype, Float remapVal) throws IOException {
		int cursor = 0;
		DEF_FILE.seek(cursor);

		DEF_FILE.writeInt(def.getSizes()[0] + 1);
		cursor += Integer.BYTES;
		DEF_FILE.seek(cursor);
		DEF_FILE.writeInt(def.getSizes()[1] + 1);
		cursor += Integer.BYTES;
		DEF_FILE.seek(cursor);
		DEF_FILE.writeInt(def.getSizes()[2] + 1);
		cursor += Integer.BYTES;
		DEF_FILE.seek(cursor);
		DEF_FILE.writeInt(def.getSizes()[3] + 1);
		cursor += Integer.BYTES;

		def.setSizes(new int[] { def.getSizes()[0] + 1, def.getSizes()[1] + 1, def.getSizes()[2] + 1,
				def.getSizes()[3] + 1 });

		DEF_FILE.seek(cursor);

		for (String cn : def.getColumns()) {
			DEF_FILE.writeChars(cn);
			cursor += 3 * Character.BYTES;
			DEF_FILE.seek(cursor);
		}

		DEF_FILE.writeChars(name.substring(0, 3));

		String[] newC = Arrays.copyOf(def.getColumns(), def.getColumns().length + 1);
		newC[def.getColumns().length] = name.substring(0, 3);

		def.setColumns(newC);

		cursor += 3 * Character.BYTES;
		DEF_FILE.seek(cursor);

		for (DataType cn : def.getDatatypes()) {
			DEF_FILE.writeShort(cn.type);
			cursor += Short.BYTES;
			DEF_FILE.seek(cursor);
		}

		DEF_FILE.writeShort(type.type);

		DataType[] newD = Arrays.copyOf(def.getDatatypes(), def.getDatatypes().length + 1);
		newD[def.getDatatypes().length] = type;

		def.setDatatypes(newD);

		cursor += Short.BYTES;
		DEF_FILE.seek(cursor);

		for (CompressionTypeEnum cn : def.getCompressionStrategies()) {
			DEF_FILE.writeShort(cn.type);
			cursor += Short.BYTES;
			DEF_FILE.seek(cursor);
		}

		DEF_FILE.writeShort(ctype.type);

		CompressionTypeEnum[] newCT = Arrays.copyOf(def.getCompressionStrategies(),
				def.getCompressionStrategies().length + 1);
		newCT[def.getCompressionStrategies().length] = ctype;

		def.setCompressionStrategies(newCT);

		cursor += Short.BYTES;
		DEF_FILE.seek(cursor);
		for (Float rv : def.getRemapperVals()) {
			DEF_FILE.writeFloat(rv);
			cursor += Float.BYTES;
			DEF_FILE.seek(cursor);
		}

		DEF_FILE.writeFloat(remapVal);

		Remapper<?, ?>[] newR = Arrays.copyOf(def.getRemapperFunctions(), def.getRemapperFunctions().length + 1);
		Float[] newRV = Arrays.copyOf(def.getRemapperVals(), def.getRemapperVals().length + 1);
		newRV[def.getRemapperVals().length] = remapVal;
		switch (ctype) {
		case DIFF:
			newR[def.getRemapperFunctions().length] = new NoopRemapper();
			break;
		case ENM:
			newR[def.getRemapperFunctions().length] = new NoopRemapper();
			break;
		case NONE:
			newR[def.getRemapperFunctions().length] = new NoopRemapper();
			break;
		case OFFST:
			newR[def.getRemapperFunctions().length] = new OffsetByRemapper(remapVal);
			break;
		case STEP:
			newR[def.getRemapperFunctions().length] = new StepRemapper(remapVal);
			break;
		default:
			break;
		}

		def.setRemapperFunctions(newR);
		def.setRemapperVals(newRV);

		return 0;
	}

	public void store(ByteStaticRecord r) throws IOException {
		long start = System.nanoTime();
		r.add("CLK", data_lines + 1);
		for (Filter f : filterChain) {
			f.doFilter(r, uncompressedLast, stats, data_lines);
		}
		long fw = System.nanoTime() - start;
		start = System.nanoTime();
		byte[] compressed = compressor.doCompress(r, def);
		D_DATA_FILE.seek(compare_data_cursor);
		D_DATA_FILE.write(compressed);
		int us = compressed.length;
		long uw = System.nanoTime() - start;
		start = System.nanoTime();
		compressor.doRemap(r, def);
		compressed = compressor.doCompress(r, def);
		compare_data_cursor += compressed.length;
		if (uncompressedLastBytes.length != 0) {
			byte[] merged = compressor.merge(compressed, uncompressedLastBytes);
			uncompressedLastBytes = compressed;
			uncompressedLast = r;
			compressed = merged;
		} else {
			uncompressedLastBytes = compressed;
			uncompressedLast = r;
		}
		if ((data_lines + 1) % dataLineCutoff == 0) {
			huffTree = compressor.constructHTree(frequencies);
			last_huffman_byte = data_cursor;
			isWritingToBuffer = true;
			frequencies = new ConcurrentHashMap<>();
			LOG.info("enter compression at {}", data_cursor);
		}

		for (byte b : compressed) {
			if (frequencies.containsKey(b)) {
				frequencies.put(b, frequencies.get(b) + 1);
			} else {
				frequencies.put(b, 1);
			}
		}


		DATA_FILE.seek(data_cursor);
		DATA_FILE.write(compressed);
		data_cursor += compressed.length;
		data_lines += 1;
		STAT_FILE.seek(0);
		STAT_FILE.writeInt(data_lines);
		STAT_FILE.seek(Integer.BYTES);
		STAT_FILE.writeInt(data_cursor);
		long cw = System.nanoTime() - start;
		LOG_FILE.seek(LOG_FILE.length());
		LOG_FILE.writeChars((data_lines + 1) + ";" + fw + ";" + uw + ";" + cw + ";" + us + ";" + compressed.length + ";\r\n");

	}

	@Override
	public void close() throws IOException {
		STAT_FILE.seek(2 * Integer.BYTES);
		STAT_FILE.writeBoolean(true);

		long curr = LOG_FILE.length();
		for (Entry<String, Object> e : stats.entrySet()) {
			LOG_FILE.seek(curr);
			LOG_FILE.writeInt(e.getKey().length() * Character.BYTES);
			curr += Integer.BYTES;
			LOG_FILE.seek(curr);
			LOG_FILE.writeChars(e.getKey());
			curr += e.getKey().length() * Character.BYTES;
			LOG_FILE.seek(curr);
			LOG_FILE.writeFloat((float) e.getValue());
			curr += Float.BYTES;
		}
		LOG.info("New struct file size:" + DATA_FILE.length());
		LOG.info("Old struct file size:" + D_DATA_FILE.length());
	}

	public void encryptSingleRowSilent() throws IOException {
		if (first_huffman_byte < last_huffman_byte) {
			LOG.info("Transcribing to hfile");
			int cmpSize = 0;
			if (first_huffman_byte != 0) {
				DATA_FILE.seek(first_huffman_byte);
				byte[] mask = new byte[Math.ceilDiv(def.getRecordSize(), 8)];
				DATA_FILE.read(mask);
				cmpSize = mask.length;
				for (int i = 0; i < def.getRecordSize(); i++) {
					if (((mask[i / 8] >> i % 8) & 1) == 1) {
						cmpSize++;
					}
				}
			} else {
				cmpSize = def.getRecordSize();
			}

			int bitcntr = 0;
			byte[] bitset = new byte[cmpSize * 2];

			byte[] row = new byte[cmpSize];
			Arrays.fill(bitset, (byte) 0);
			DATA_FILE.seek(first_huffman_byte);
			DATA_FILE.read(row);
			for (int i=0;i<row.length;i++) {
				String code = huffTree.findCodeByValue(row[i]);
				for (char c : code.toCharArray()) {
					if (c == '0') {
						int shiftBy = bitcntr % 8;
						bitset[bitcntr/8] = (byte) ((bitset[bitcntr/8]) & ~(1 << shiftBy));
					} else {
						int shiftBy = bitcntr % 8;
						bitset[bitcntr/8] = (byte) ((bitset[bitcntr/8]) | (1 << shiftBy));
					}
					
					bitcntr++;
				}
			}
            // TODO figure out endianess issue BitSet is little endian, should use bigendian
			row = Arrays.copyOf(bitset, Math.ceilDiv(bitcntr, 8));
			LOG.info("{}", huff_curr);
			LOG.info("{} {}", (byte) (cmpSize >> 0), (byte) (row.length >> 0));
			HBUF_FILE.seek(huff_last_line + huff_curr);
			HBUF_FILE.writeByte(cmpSize >> 0);
			huff_curr += 1;
			HBUF_FILE.seek(huff_last_line + huff_curr);
			HBUF_FILE.writeByte(row.length >> 0);
			huff_curr += 1;
			first_huffman_byte += cmpSize;
			HBUF_FILE.seek(huff_last_line + huff_curr);
			HBUF_FILE.write(row);
			huff_curr += row.length;
		} else {
			LOG.info("{}", huff_curr);
			isWritingToBuffer = false;
			isBufferingData = true;
		}
	}

	public void resetHuffExecution() throws IOException {
		long ptr = HMAP_FILE.length();
		HMAP_FILE.seek(ptr);
		HMAP_FILE.writeInt((huffTree.getFrequencies().entrySet().size() * (Byte.BYTES + Integer.BYTES)) + Integer.BYTES);
		ptr += Integer.BYTES;
		HMAP_FILE.seek(ptr);
		HMAP_FILE.writeInt(huff_curr);
		ptr += Integer.BYTES;
		HMAP_FILE.seek(ptr);
		for (Entry<Byte, Integer> e : huffTree.getFrequencies().entrySet()) {
			HMAP_FILE.writeByte(e.getKey());
			ptr += Byte.BYTES;
			HMAP_FILE.seek(ptr);
			HMAP_FILE.writeInt(e.getValue());
			ptr += Integer.BYTES;
			HMAP_FILE.seek(ptr);
		}

		int newlyAddedStuff = data_cursor - last_huffman_byte;
		byte[] toCopy = new byte[newlyAddedStuff];
		DATA_FILE.seek(last_huffman_byte);
		DATA_FILE.read(toCopy);
		data_cursor = 0;
		DATA_FILE.seek(data_cursor);
		for (byte b : toCopy) {
		    DATA_FILE.writeByte(b);
		    ++data_cursor;
		}
		huff_curr = 0;
		first_huffman_byte = 0;
		last_huffman_byte = 0;
		huff_last_line = HBUF_FILE.length();
		isBufferingData = false;
	}

	public boolean isNotEmptyFile() {
		return isNotEmptyFile;
	}

	public void addFilter(Filter f) {
		filterChain.add(f);
	}

	public boolean isWritingToBuffer() {
		return isWritingToBuffer;
	}

	public void setWritingToBuffer(boolean isWritingToBuffer) {
		this.isWritingToBuffer = isWritingToBuffer;
	}

	public boolean isBufferingData() {
		return isBufferingData;
	}

	public void setBufferingData(boolean isBufferingData) {
		this.isBufferingData = isBufferingData;
	}

}