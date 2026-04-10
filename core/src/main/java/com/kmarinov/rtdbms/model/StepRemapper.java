package com.kmarinov.rtdbms.model;

public class StepRemapper implements Remapper<Integer, Float>{
	
	private final Float step;
	
	public StepRemapper(Float step) {
		this.step = step;
	}

	@Override
	public Integer doRemap(Float from) {
		
		return (int) Math.ceil(from/step);
	}

}
