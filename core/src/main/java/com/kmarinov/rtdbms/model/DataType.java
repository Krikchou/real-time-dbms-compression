package com.kmarinov.rtdbms.model;

public enum DataType {
	STR(0),
	NUM(1),
	FPN(2),
	FLG(3),
	BDC(4);
	
	public int type;
	
	DataType(int type) {
		this.type = type;
	}
	
	public static DataType valueOf(int flg) {
		for(DataType t : DataType.values()) {
			if (t.type == flg) {
				return t;
			}
		}
		
		return null;
	}

}
