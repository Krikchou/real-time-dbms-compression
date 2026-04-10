package com.kmarinov.rtdbms.model;

public class OffsetByRemapper implements Remapper<Float, Float> {
	
	private final Float offsetFrom;

	public OffsetByRemapper(Float from) {
		this.offsetFrom = from;
	}
	
	@Override
	public Number doRemap(Number from) {
		return (Float) from - offsetFrom;
	}

	@Override
	public Number restore(Number from) {
		return (Float) from + offsetFrom;
	}
	

}
