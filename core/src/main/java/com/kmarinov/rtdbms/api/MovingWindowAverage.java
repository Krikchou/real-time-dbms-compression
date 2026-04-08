package com.kmarinov.rtdbms.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import com.kmarinov.rtdbms.model.ByteStaticRecord;

public class MovingWindowAverage implements Filter {
	private static final String PREFIX = "WAVG_";
	private static final int WIN_LEN = 5;
	private static Map<String, Queue<Number>> cache = new HashMap<>();

	@Override
	public void doFilter(ByteStaticRecord curr, ByteStaticRecord last, Map<String, Object> stats, int lines) {
		for (Entry<String, Object> me : curr.getValues().entrySet()) {
			if (me.getKey()!= "CLK" && me.getValue() instanceof Number currVal) {
				switch (currVal) {
				case Integer i -> {
					if (cache.containsKey(PREFIX + me.getKey())) {
						Float stat = (Float) stats.getOrDefault(PREFIX + me.getKey(), 0f);
						Integer out = (Integer) cache.get(me.getKey()).poll();
						cache.get(me.getKey()).offer(i);
						stats.put(PREFIX + me.getKey(), ((WIN_LEN) * stat - (out.floatValue() / (float) WIN_LEN) + (i.floatValue() / (float) WIN_LEN))); 
					} else {
						Queue<Integer> q = new SynchronousQueue<Integer>();
						for (int j = 0;j<WIN_LEN-1;j++) {
							q.offer(0);
						}
						q.offer(i);
						
						stats.put(PREFIX + me.getKey(), (i.floatValue()/ ((float) WIN_LEN)));
					}
				}
				case Float f -> {
					if (cache.containsKey(PREFIX + me.getKey())) {
						Float stat = (Float) stats.getOrDefault(PREFIX + me.getKey(), 0f);
						Float out = (Float) cache.get(me.getKey()).poll();
						cache.get(me.getKey()).offer(f);
						stats.put(PREFIX + me.getKey(), ((WIN_LEN) * stat - (out.floatValue() / (float) WIN_LEN) + (f.floatValue() / (float) WIN_LEN))); 
					} else {
						Queue<Float> q = new SynchronousQueue<Float>();
						for (int j = 0;j<WIN_LEN-1;j++) {
							q.offer(0f);
						}
						q.offer(f);
						
						stats.put(PREFIX + me.getKey(), (f.floatValue()/ ((float) WIN_LEN)));
					}
				}
				case Short s -> {
					if (cache.containsKey(PREFIX + me.getKey())) {
						Float stat = (Float) stats.getOrDefault(PREFIX + me.getKey(), 0f);
						Short out = (Short) cache.get(me.getKey()).poll();
						cache.get(me.getKey()).offer(s);
						stats.put(PREFIX + me.getKey(), ((WIN_LEN) * stat - (out.floatValue() / (float) WIN_LEN) + (s.floatValue() / (float) WIN_LEN))); 
					} else {
						Queue<Short> q = new SynchronousQueue<Short>();
						for (int j = 0;j<WIN_LEN-1;j++) {
							q.offer((short) 0);
						}
						q.offer(s);
						
						stats.put(PREFIX + me.getKey(), (s.floatValue()/ ((float) WIN_LEN)));
					}
				}
				default -> throw new IllegalArgumentException("Unexpected value: " + currVal);
				};
				
				curr.add("W" + me.getKey().substring(0, 2), stats.get(PREFIX + me.getKey()));
			}
		}
		
	}

}
