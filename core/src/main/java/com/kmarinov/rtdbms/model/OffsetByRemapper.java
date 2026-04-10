package com.kmarinov.rtdbms.model;

public class OffsetByRemapper implements Remapper<Float, Float> {
	
	private final Float offsetFrom;

	public OffsetByRemapper(Float from) {
		this.offsetFrom = from;
	}
	
	@Override
	public Float doRemap(Float from) {
		return from - offsetFrom;
	}
	

}
