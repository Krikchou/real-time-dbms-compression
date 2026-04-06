package com.kmarinov.rtdbms.manager;

public class HTNode {
  	// Character data
    byte data;           
  	
  	// Frequency of the character
    int frequency;       
  
  	// Left and right child nodes
    HTNode left, right; 

    // Constructor to initialize the node
    HTNode(byte data, int frequency) {
        this.data = data;
        this.frequency = frequency;
        left = right = null;
    }
}
