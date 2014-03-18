package com.imgraph.index;

@SuppressWarnings("hiding")
public class Pair<Object> { 
	public final Object attribute; 
	public final Object value; 
	public Pair(Object attribute, Object value) { 
		this.attribute = attribute; 
		this.value = value; 
	} 
	
	public String toString() {
		return "{"+attribute+","+value+"}";
	}
} 