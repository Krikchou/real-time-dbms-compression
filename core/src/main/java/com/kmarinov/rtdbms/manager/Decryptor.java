package com.kmarinov.rtdbms.manager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

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
	private static RandomAccessFile TEMP_FILE;
	private static RandomAccessFile D_DATA_FILE;
	private static RandomAccessFile DEF_FILE;
	private static RandomAccessFile DUMP_FILE;
	private static RandomAccessFile HBUF_FILE;
	private static RandomAccessFile HMAP_FILE;
	private static RandomAccessFile STAT_FILE;
	private int data_lines;
	private int data_cursor;
	private DatabaseDefinition def;
	private int rec_cur;
	private int rec_ext;

	private static final Logger LOG = LoggerFactory.getLogger(Decryptor.class);

	public Decryptor(String rootDir) throws IOException {
		DATA_FILE = new RandomAccessFile(rootDir + RTManager.DAF_COM_N, "rw");
		D_DATA_FILE = new RandomAccessFile(rootDir + RTManager.CMP_COM_N, "rw");
		DEF_FILE = new RandomAccessFile(rootDir + RTManager.DEF_COM_N, "rw");
		DUMP_FILE = new RandomAccessFile(rootDir + "/datadump.txt", "rw");
		TEMP_FILE = new RandomAccessFile(rootDir + "/temp", "rw");
		HBUF_FILE = new RandomAccessFile(rootDir + RTManager.HCF_COM_N, "rw");
		HMAP_FILE = new RandomAccessFile(rootDir + RTManager.HSF_COM_N, "rw");
		STAT_FILE = new RandomAccessFile(rootDir + RTManager.STF_COM_N, "rw");

		this.readDefinitionFile();
		this.readStatFile();
	}
	
	private void readStatFile() throws IOException {
		int cursor = 0;
		STAT_FILE.seek(cursor);
		byte[] buff = new byte[2 * Integer.BYTES + 1];
		STAT_FILE.read(buff);
		data_lines = ByteBuffer.wrap(buff, 0, 4).getInt();
		data_cursor = ByteBuffer.wrap(buff, 4, 4).getInt();
	}

	public static Decryptor instance(String root) throws IOException {
		return new Decryptor(root);
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

	public void validate() throws IOException {
		LOG.info("Start data consistency validation.");
		readAndCopyHuffmanFile();
		rec_cur = 0;
		rec_ext = 0;
		TEMP_FILE.seek(rec_cur);
		D_DATA_FILE.seek(rec_ext);
		byte[] recordCmp = new byte[def.getRecordSize()];
		byte[] recordExt = new byte[def.getRecordSize()];

		TEMP_FILE.read(recordCmp);
		D_DATA_FILE.read(recordExt);

		rec_cur += def.getRecordSize();
		rec_ext += def.getRecordSize();

		if (!toData(recordCmp).equals(toData(recordExt))) {
			LOG.error("Wrong assertions Expected: {} Recieved: {}", toData(recordCmp), toData(recordExt));
		}

		while (rec_cur < TEMP_FILE.length() && rec_ext < D_DATA_FILE.length()) {
			TEMP_FILE.seek(rec_cur);
			byte[] mask = new byte[Math.ceilDiv(def.getRecordSize(), 8)];
			TEMP_FILE.read(mask);
			rec_cur += mask.length;
			int cmpSize = 0;
			for (int i = 0; i < def.getRecordSize(); i++) {
				if (((mask[i / 8] >> i % 8) & 1) == 1) {
					cmpSize++;
				}
			}

			byte[] compressed = new byte[cmpSize];
			TEMP_FILE.seek(rec_cur);
			TEMP_FILE.read(compressed);
			rec_cur += cmpSize;

			cmpSize = 0;
			for (int i = 0; i < def.getRecordSize(); i++) {
				if (((mask[i / 8] >> i % 8) & 1) == 1) {
					recordCmp[i] = compressed[cmpSize];
					cmpSize++;
				}
			}

			D_DATA_FILE.seek(rec_ext);
			D_DATA_FILE.read(recordExt);
			rec_ext += def.getRecordSize();
			
			LOG.info("Expected: {} Recieved: {}", toData(recordExt), toData(recordCmp));

			if (!toData(recordCmp).equals(toData(recordExt))) {
				LOG.error("Wrong assertions Expected: {} Recieved: {}", toData(recordCmp), toData(recordExt));
			}

			DUMP_FILE.writeChars(toData(recordCmp).dumpCSVLine(def.getColumns()));
		}

		LOG.info("Data consistent after decompression with uncompressed data");
	}

	public ByteStaticRecord toData(byte[] array) {
		ByteStaticRecord record = ByteStaticRecord.getInstance();
		int offset = 0;
		for (int i = 0; i < def.getColumns().length; i++) {
			int len = 0;
			switch (def.getDatatypes()[i]) {
			case BDC:
				len = 99;
				record.add(def.getColumns()[i], ByteBuffer.wrap(array, offset, len).getDouble());
				break;
			case FLG:
				len = Short.BYTES;
				record.add(def.getColumns()[i],
						def.getRemapperFunctions()[i].restore(ByteBuffer.wrap(array, offset, len).getShort()));
				break;
			case FPN:
				len = Float.BYTES;
				record.add(def.getColumns()[i],
						def.getRemapperFunctions()[i].restore(ByteBuffer.wrap(array, offset, len).getFloat()));
				break;
			case NUM:
				len = Integer.BYTES;
				record.add(def.getColumns()[i],
						def.getRemapperFunctions()[i].restore(ByteBuffer.wrap(array, offset, len).getInt()));
				break;
			case STR:
				len = Character.BYTES * 3;
				record.add(def.getColumns()[i], String.valueOf(ByteBuffer.wrap(array, offset, len).array()));
				break;
			default:
				break;
			}
			;

			offset += len;
		}

		return record;

	}

	private void readAndCopyHuffmanFile() throws IOException {
		long rec_curr = 0;
		long hfile_curr = 0;
		long tfile_curr = 0;
		Compressor c = new Compressor();
		while (rec_curr < HMAP_FILE.length()) {
			LOG.info("ANOTHER TREE");
			HMAP_FILE.seek(rec_curr);
			int frequencyMapSize = HMAP_FILE.readInt();
			rec_curr += Integer.BYTES;
			HMAP_FILE.seek(rec_curr);
			int mappings = HMAP_FILE.readInt();
			rec_curr += Integer.BYTES;
			HMAP_FILE.seek(rec_curr);

			Map<Byte, Integer> huffMap = new HashMap<>();
			int mapIdx = 0;
			while (mapIdx < frequencyMapSize - Integer.BYTES) {
				byte toEncode = HMAP_FILE.readByte();
				rec_curr += Byte.BYTES;
				mapIdx += Byte.BYTES;
				HMAP_FILE.seek(rec_curr);
				int frequency = HMAP_FILE.readInt();
				rec_curr += Integer.BYTES;
				mapIdx += Integer.BYTES;
				HMAP_FILE.seek(rec_curr);
				huffMap.put(toEncode, frequency);
			}

			HuffmanStats stats = c.constructHTree(huffMap);
			LOG.info("GENERATED TREE");
			LOG.info("ROWS_TO_READ {}", mappings);

			int accum = 0;
			while (hfile_curr < mappings) {
				LOG.info("PTR {}", hfile_curr);
				HBUF_FILE.seek(hfile_curr);
				//uncompressed record size
				int uncRowSize = HBUF_FILE.readByte();
				hfile_curr += Byte.BYTES;
				HBUF_FILE.seek(hfile_curr);
				//compressed record size
				int rowSize = HBUF_FILE.readByte();
				hfile_curr += Byte.BYTES;
				//start of compressed record
				HBUF_FILE.seek(hfile_curr);
				LOG.info("ROW SIZE CM {} ROW SIZE UNCM {}", rowSize, uncRowSize);
				byte[] row = new byte[rowSize];
				HBUF_FILE.read(row);
				hfile_curr += rowSize;
				accum += uncRowSize;
				HuffmanNode current = stats.getRoot();
				int written_byte_cntr = 0;
				StringBuilder b = new StringBuilder();
				for (int i = 0; i < rowSize * 8 && written_byte_cntr < uncRowSize; i++) {
					if (((row[i / 8] >> i % 8) & 1) == 1) {
						current = current.right;
						b.append("1");
					} else {
						b.append("0");
						current = current.left;
					}

					if (current.isLeaf()) {
						LOG.info("Decrypt code {} into byte value {}", b.toString(), String.format("%8s", Integer.toBinaryString(current.getData().byteValue() & 0xFF)).replace(' ', '0'));
						b = new StringBuilder();
						TEMP_FILE.seek(tfile_curr);
						TEMP_FILE.writeByte(current.getData().byteValue());
						tfile_curr += Byte.BYTES;
						written_byte_cntr += Byte.BYTES;
						current = stats.getRoot();
					}
				}
			}
		}
		
		LOG.info("{}", data_cursor);
		DATA_FILE.seek(0);
		TEMP_FILE.seek(tfile_curr);
		for (int i=0;i<data_cursor;i++) {
			DATA_FILE.seek(i);
			TEMP_FILE.seek(tfile_curr);
			TEMP_FILE.writeByte(DATA_FILE.readByte());
			++tfile_curr;
		}
	}
}
