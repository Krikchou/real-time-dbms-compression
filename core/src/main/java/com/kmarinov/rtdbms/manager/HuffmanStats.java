package com.kmarinov.rtdbms.manager;

import java.util.Map;

public class HuffmanStats {
 Map<Byte, Integer> frequencies;
 Map<Byte, String> codes;
 HuffmanNode root;
 
 public HuffmanStats(HuffmanNode root, Map<Byte, String> codes, Map<Byte, Integer> freq) {
	 this.root = root;
	 this.codes = codes;
	 this.frequencies = freq;
 }
 
 public Map<Byte, String> getCodes() {
	return codes;
}
 
 public String findCodeByValue(Byte b) {
	return codes.entrySet().stream().filter(e -> (e.getKey().byteValue() ^ b.byteValue()) == 0).findFirst().get().getValue();
 }
 
 public void setCodes(Map<Byte, String> codes) {
	this.codes = codes;
 }
 public HuffmanNode getRoot() {
	return root;
 }
 public void setRoot(HuffmanNode root) {
	this.root = root;
 }

 public Map<Byte, Integer> getFrequencies() {
	return frequencies;
 }

 public void setFrequencies(Map<Byte, Integer> frequencies) {
	this.frequencies = frequencies;
 }
}
