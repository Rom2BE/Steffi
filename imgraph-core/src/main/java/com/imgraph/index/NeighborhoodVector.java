package com.imgraph.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 
 * @author Romain Capron
 * Every vertex stores information about its neighborhood in a NeighbourhoodVector
 */
public class NeighborhoodVector implements Serializable{

	private static final long serialVersionUID = 766758468137921169L;
	private Map<String, List<Tuple<Object, Integer>>> vector;
	
	public NeighborhoodVector() {
		vector = new HashMap<String, List<Tuple<Object, Integer>>>();
	}
	
	
	public NeighborhoodVector(Map<String, List<Tuple<Object, Integer>>> vector) {
		this.vector = vector;
	}
	
	
	public Map<String, List<Tuple<Object, Integer>>> getVector() {
		return vector;
	}
	
	
	public void setVector(Map<String, List<Tuple<Object, Integer>>> vector) {
		this.vector = vector;
	}
	
	/**
	 * TODO
	 * @param neighborhoodVector
	 * @param modificationsNeeded
	 * @return
	 */
	public static NeighborhoodVector applyModifications(
			NeighborhoodVector neighborhoodVector, 
			Map<String, List<Tuple<Object, 
			Integer>>> modificationsNeeded) {
		Map<String, List<Tuple<Object, Integer>>> vector = neighborhoodVector.getVector();
		
		//Iterate modifications' Attributes
		for (Entry<String, List<Tuple<Object, Integer>>> modificationsEntry : modificationsNeeded.entrySet()){
			List<Tuple<Object, Integer>> tupleToRemove = new ArrayList<Tuple<Object, Integer>>();
			
			boolean attributePresent = false;
			//Find the corresponding Attribute in vector
			for (Entry<String, List<Tuple<Object, Integer>>> vectorEntry : vector.entrySet()){
				if (vectorEntry.getKey().equals(modificationsEntry.getKey())){
					attributePresent = true;
					
					//Iterate modifications' Attributes' values
					for (Tuple<Object, Integer> modificationsTuple : modificationsEntry.getValue()){
						
						boolean valuePresent = false;
						//Find the corresponding Attribute's value in vector
						for (Tuple<Object, Integer> vectorTuple : vectorEntry.getValue()){
							if (vectorTuple.getX().equals(modificationsTuple.getX())){
								valuePresent = true;
										
								//Apply modification
								vectorTuple.setY(vectorTuple.getY() + modificationsTuple.getY());
								
								//Remove this tuple if the intensity equals 0
								if (vectorTuple.getY().equals(0))
									tupleToRemove.add(vectorTuple);
							}
						}
						
						/*
						 * Value seen for the first time
						 */
						if (!valuePresent){
							List<Tuple<Object, Integer>> list = vector.get(vectorEntry.getKey());
							list.add(modificationsTuple);
							vector.put(modificationsEntry.getKey(), list);
						}
					}
				}
			}
			
			/*
			 * Attribute seen for the first time
			 */
			if (!attributePresent){
				vector.put(modificationsEntry.getKey(), modificationsEntry.getValue());
			}
			
			/*
			 * Apply removals
			 */
			for (Tuple<Object, Integer> t : tupleToRemove){
				vector.get(modificationsEntry.getKey()).remove(t);
				if (vector.get(modificationsEntry.getKey()).size() == 0)
					vector.remove(modificationsEntry.getKey());
			}	
		}
		
		return new NeighborhoodVector(vector);
	}
	
	
	public String toString() {
		String neighboursList = "\t - Neighborhood vector :";
		for (Entry<String, List<Tuple<Object, Integer>>> entry : vector.entrySet()){
			neighboursList += "\n\t\t - "+entry.getKey()+" : [";
			for (Tuple<Object, Integer> tuple : entry.getValue()){
				neighboursList += "{"+tuple.getX()+","+tuple.getY()+"},";
			}
			neighboursList = neighboursList.substring(0, neighboursList.length()-1);
			neighboursList += "]";
		}
		return neighboursList;
	}
}