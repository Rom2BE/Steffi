/**
 * 
 */
package com.imgraph.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.imgraph.model.ImgGraph;

/**
 * @author Romain Capron
 * An index that store intensities of attributes found in neighborhood vectors
 */
public class AttributeIndex implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8532530566308216760L;
	private Map<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndex;
	
	public AttributeIndex() {
		attributeIndex = new HashMap<String, Map<Object, List<Tuple<Long, Integer>>>>();
	}
	
	public AttributeIndex(HashMap<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndex) {
		this.attributeIndex = attributeIndex;
	}
	
	public Map<String, Map<Object, List<Tuple<Long, Integer>>>> getAttributeIndex(){
		return attributeIndex;
	}
	
	public void setAttributeIndex(Map<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndex){
		this.attributeIndex = attributeIndex;
	}
	
	/**
	 * Only add information (faster)
	 */
	public static void updateAttributeIndex(){
		Map<Long, NeighborhoodVector> vectorsMap = ImgGraph.getInstance().getNeighborhoodVectorMap();
		Map<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndex = ImgGraph.getInstance().getAttributeIndex().getAttributeIndex();
		//System.out.println("NeighborhoodVectorMap : " + ImgGraph.getInstance().getNeighborhoodVectorMap());
		//System.out.println("BEFORE : " + ImgGraph.getInstance().getAttributeIndex().getAttributeIndex());
		for (Entry<Long, NeighborhoodVector> entry : vectorsMap.entrySet()){
			Map<String, List<Tuple<Object, Integer>>> vector = entry.getValue().getVector();
			
			//Each NeighborhoodVector is composed of a list of Tuple<Object, Integer>
			for (Entry<String, List<Tuple<Object, Integer>>> attributeValue : vector.entrySet()){
				Map<Object, List<Tuple<Long, Integer>>> objectMapInAttributesMap = attributeIndex.get(attributeValue.getKey());
				List<Tuple<Long, Integer>> tupleListInAttributesMap;
				//Attribute seen for the first time
				if (objectMapInAttributesMap == null)
					objectMapInAttributesMap = new HashMap<Object, List<Tuple<Long, Integer>>>();
				
				for(Tuple<Object, Integer> tuple : attributeValue.getValue()){
					tupleListInAttributesMap = objectMapInAttributesMap.get(tuple.getX());
					//Value for this attribute seen for the first time
					if (tupleListInAttributesMap == null)
						tupleListInAttributesMap = new ArrayList<Tuple<Long, Integer>>();
					
					Tuple<Long, Integer> newTuple = new Tuple<Long, Integer>(entry.getKey(), tuple.getY());
					
					//Check if already there
					boolean present = false;
					for (Tuple<Long, Integer> t : tupleListInAttributesMap){
						if (t.getX().equals(newTuple.getX())){
							present = true;
							t.setY(newTuple.getY());
						}
					}
					if (!present)
						tupleListInAttributesMap.add(newTuple);
					
					objectMapInAttributesMap.put(tuple.getX(), tupleListInAttributesMap);
				}
				attributeIndex.put(attributeValue.getKey(), objectMapInAttributesMap);
			}
		}
		
		ImgGraph.getInstance().getAttributeIndex().setAttributeIndex(attributeIndex);
		//System.out.println("AFTER : " + ImgGraph.getInstance().getAttributeIndex().getAttributeIndex());
	}
	
	public static void removeOldIdsAttributeIndex(Long id, NeighborhoodVector neighborhoodVector){
		Map<String, List<Tuple<Object, Integer>>> vector = neighborhoodVector.getVector();
		Map<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndex = ImgGraph.getInstance().getAttributeIndex().getAttributeIndex();
		Map<String, Map<Object, List<Tuple<Long, Integer>>>> result = ImgGraph.getInstance().getAttributeIndex().getAttributeIndex();
		
		System.out.println(id + " will be remove with these attributes : "+neighborhoodVector);
		
		for (Entry<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndexEntry : attributeIndex.entrySet()){
			
			//Check if there are values (Object) in this attribute (String) we need to remove
			System.out.println(vector.keySet().contains(attributeIndexEntry.getKey()) + " vector.keySet().contains(attributeIndexEntry.getKey() : " + attributeIndexEntry.getKey());
			if(vector.keySet().contains(attributeIndexEntry.getKey())){
				
				boolean attributeIndexEntryModified = false;
				List<Object> objectRemoved = new ArrayList<Object>();
				
				for (Entry<Object, List<Tuple<Long, Integer>>> attributeIndexEntryValue : attributeIndexEntry.getValue().entrySet()){
					
					//Check if this value (Object) is present in the removed vertex
					boolean present = false;
					for (Tuple<Object, Integer> tuple : vector.get(attributeIndexEntry.getKey())){
						System.out.println("Object : " + tuple.getX());
						if (tuple.getX().equals(attributeIndexEntryValue.getKey()))
							present = true;
					}
					System.out.println("Present : " + present);
					//Search after possible tuple to remove
					boolean modified = false;
					List<Tuple<Long, Integer>> tupleRemoved = new ArrayList<Tuple<Long, Integer>>();
					if (present){
						for (Tuple<Long, Integer> tuple : attributeIndexEntryValue.getValue()){
							if (tuple.getX().equals(id)){
								System.out.println("tupleRemoved.add(tuple) "+tuple);
								tupleRemoved.add(tuple);
								modified = true;
							}
						}
					}
					
					//Process these modifications
					if (modified){
						attributeIndexEntryModified = true;
						for (Tuple<Long, Integer> tuple : tupleRemoved){
							System.out.println("removing "+tuple);
							attributeIndexEntryValue.getValue().remove(tuple);
						}
						if (attributeIndexEntryValue.getValue().size() == 0)
							objectRemoved.add(attributeIndexEntryValue.getKey());
					}
				}
				
				for (Object objectToRemove : objectRemoved)
					attributeIndexEntry.getValue().remove(objectToRemove);
				
				if (attributeIndexEntryModified){
					result.put(attributeIndexEntry.getKey(), attributeIndexEntry.getValue());
				}
			}
		}
		System.out.println("RESULT = " + result);
		ImgGraph.getInstance().getAttributeIndex().setAttributeIndex(result);
	}
	
	/**
	 * 
	 * @param vm1 newer values
	 * @param vm2 
	 * @return vm2 with the values of vm1
	 */
	public static Map<Long, NeighborhoodVector> mergeNeighborhoodVectorMap (Map<Long, NeighborhoodVector> vm1, Map<Long, NeighborhoodVector> vm2){
		for (Entry<Long, NeighborhoodVector> entryVM1 : vm1.entrySet()){
			vm2.put(entryVM1.getKey(), entryVM1.getValue());
		}
		return vm2;
	}

	@SuppressWarnings("unchecked")
	public String toString() {
		String result = "\nAttribute Index : \n";
		String valueIntensities = "";
		for (Entry<String, Map<Object, List<Tuple<Long, Integer>>>> entry : attributeIndex.entrySet()){
			result += entry.getKey() + ":\n";
			for (Entry<Object, List<Tuple<Long, Integer>>> attributeValue : entry.getValue().entrySet()){
				result += "\t"+attributeValue.getKey() + " : ";
				valueIntensities = "[";
				//Sort intensities in decreasing order
				List<Tuple<Long, Integer>> tupleList = attributeValue.getValue();
				//Sorting tuples on custom order defined by TupleComparator
		        Collections.sort(tupleList,new TupleComparator());
		      
		        for(Tuple<Long, Integer> tuple : tupleList){
					valueIntensities += tuple+", ";
				}
				valueIntensities = valueIntensities.substring(0, valueIntensities.length()-2);
				result += valueIntensities + "]\n";
			}
		}
		return result;
	}
	
}
