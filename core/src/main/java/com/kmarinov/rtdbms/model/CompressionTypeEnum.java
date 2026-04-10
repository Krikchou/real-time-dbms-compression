package com.kmarinov.rtdbms.model;

public enum CompressionTypeEnum {
	NONE(0),
	DIFF(1),
	ENM(2),
	STEP(3),
	OFFST(4);
	
	public int type;
	
	CompressionTypeEnum(int type) {
		this.type = type;
	}
	
	public static CompressionTypeEnum valueOf(int flg) {
		for(CompressionTypeEnum t : CompressionTypeEnum.values()) {
			if (t.type == flg) {
				return t;
			}
		}
		
		return null;
	}

}
