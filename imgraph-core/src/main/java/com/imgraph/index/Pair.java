package com.imgraph.index;

import java.io.Serializable;

@SuppressWarnings("hiding")
public class Pair<Object>  implements Serializable{ 
	/**
	 * 
	 */
	private static final long serialVersionUID = -5774986057801558032L;
	private final Object attribute; 
	private Object value; 
	public Pair(Object attribute, Object value) { 
		this.attribute = attribute; 
		this.value = value; 
	} 
	public Object getAttribute(){
		return this.attribute;
	}
	
	public Object getValue(){
		return this.value;
	}
	
	public void setValue(Object value){
		this.value = value;
	}
	
	public String toString() {
		return "{"+attribute+","+value+"}";
	}
} 