package com.kmarinov.rtdbms.model;

public class DatabaseDefinition {
	private int[] sizes;
	private String[] columns;
	private DataType[] datatypes;
	private CompressionTypeEnum[] compressionStrategies;
	private int recordSize;

	public String[] getColumns() {
		return columns;
	}

	public void setColumns(String[] columns) {
		this.columns = columns;
	}

	public DataType[] getDatatypes() {
		return datatypes;
	}

	public void setDatatypes(DataType[] datatypes) {
		this.datatypes = datatypes;
		this.calcMaxRecordSize();
	}

	public CompressionTypeEnum[] getCompressionStrategies() {
		return compressionStrategies;
	}

	public void setCompressionStrategies(CompressionTypeEnum[] compressionStrategies) {
		this.compressionStrategies = compressionStrategies;
	}

	public int[] getSizes() {
		return sizes;
	}

	public void setSizes(int[] sizes) {
		this.sizes = sizes;
	}
	
	public int getRecordSize() {
		return this.recordSize;
	}

	private void calcMaxRecordSize() {
		int sum = 0;
		for (DataType dt : datatypes) {
			switch (dt) {
			case FLG:
                sum += Short.BYTES;
				break;
			case FPN:
				sum += Float.BYTES;
				break;
			case NUM:
				sum += Integer.BYTES;
				break;
			case STR:
				sum += 5*Character.BYTES;
				break;
			}
		}
		
		this.recordSize = sum;
	}
}
