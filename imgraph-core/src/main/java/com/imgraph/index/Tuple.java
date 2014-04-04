package com.imgraph.index;

import java.io.Serializable;

@SuppressWarnings("rawtypes")
public class Tuple<X, Y> implements Serializable, Comparable{ 
	
	private static final long serialVersionUID = 205230036159116313L;
	private X x; 
	private Y y; 
	public Tuple(X x, Y y) { 
		this.x = x; 
		this.y = y; 
	} 
	
	public X getX(){
		return x;
	}
	
	public void setX(X x){
		this.x = x;
	}
	
	public Y getY(){
		return y;
	}
	
	public void setY(Y y){
		this.y = y;
	}
	
	public String toString(){
		return "{"+x+","+y+"}";
	}
	
	@Override
	public int compareTo(Object tuple) {
		return ((Integer) this.y).compareTo((Integer) ((Tuple) tuple).getY());
	}
} 