package com.kmarinov.rtdbms.model;

public class StepRemapper implements Remapper<Integer, Float>{
	
	private final Float step;
	
	public StepRemapper(Float step) {
		this.step = step;
	}

	@Override
	public Number doRemap(Number from) {
		
		return (int) Math.ceil((Float) from/step);
	}

	@Override
	public Float restore(Number number) {
		return ((Integer) number * step);
	}

}
