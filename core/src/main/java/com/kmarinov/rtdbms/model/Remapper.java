package com.kmarinov.rtdbms.model;

public interface Remapper<T extends Number, P extends Number> {
	Number doRemap(Number number);
	Number restore(Number number);
}
