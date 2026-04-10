package com.kmarinov.rtdbms.model;

public class NoopRemapper implements Remapper<Number, Number> {

	@Override
	public Number doRemap(Number from) {
		return from;
	}

	@Override
	public Number restore(Number number) {
		return number;
	}

}
