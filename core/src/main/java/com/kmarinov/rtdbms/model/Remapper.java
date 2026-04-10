package com.kmarinov.rtdbms.model;

public interface Remapper<T extends Number, P extends Number> {
	T doRemap(P from);
}
