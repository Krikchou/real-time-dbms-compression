package com.kmarinov.rtdbms.manager;

import com.kmarinov.rtdbms.manager.TreePrinter.PrintableNode;

class HuffmanNode implements PrintableNode {
	// Character data
	Byte data;

	// Frequency of the character
	int frequency;

	// Left and right child nodes
	HuffmanNode left, right;

	String code;

	// Constructor to initialize the node
	HuffmanNode(Byte data, int frequency) {
		this.data = data;
		this.frequency = frequency;
		left = right = null;
	}

	public void setCode(String code) {
		this.code = code;
	}
	
	public boolean isLeaf() {
		return left == null && right == null;
	}

	@Override
	public PrintableNode getLeft() {
		return left;
	}

	@Override
	public PrintableNode getRight() {
		return right;
	}

	@Override
	public String getText() {
		if (data != null) {
			return String.format("%8s", Integer.toBinaryString(this.data & 0xFF)).replace(' ', '0') + ": " + code;
		} else {
			return "0|1";
		}
	}

	public Byte getData() {
		return data;
	}

	public void setData(Byte data) {
		this.data = data;
	}
}
