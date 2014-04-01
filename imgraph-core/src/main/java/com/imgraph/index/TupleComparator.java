package com.imgraph.index;

import java.util.Comparator;

public class TupleComparator implements Comparator{
	//Used to sort a list of tuples in decreasing order
	@Override
	public int compare(Object tuple1, Object tuple2) { //TODO check total value (25+50) (sort again after addition)
		return ((Integer) ((Tuple) tuple1).getY() > (Integer) ((Tuple) tuple2).getY() ) ? -1: ((Integer) ((Tuple) tuple1).getY() < (Integer) ((Tuple) tuple2).getY() ) ? 1:0 ;
	}
}