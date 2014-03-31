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

import com.imgraph.model.ImgEdge;
import com.imgraph.model.ImgGraph;
import com.imgraph.model.ImgVertex;
import com.imgraph.storage.CacheContainer;

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
	
	public static List<Tuple<Long, Map<String, List<Tuple<Object, Integer>>>>> updateList(Long id, String attribute, Object value, int modification, List<Tuple<Long, Map<String, List<Tuple<Object, Integer>>>>> list){
		
		boolean vertexFound = false;
		for(Tuple<Long, Map<String, List<Tuple<Object, Integer>>>> tuple : list){
			
			//Vertex Found
			if (tuple.getX().equals(id)){
				vertexFound = true;
				
				boolean attributeFound = false;
				for (Entry<String, List<Tuple<Object, Integer>>> attributeSaved : tuple.getY().entrySet()){
					
					//Attribute Found
					if (attributeSaved.getKey().equals(attribute)){
						attributeFound = true;
						
						boolean valueFound = false;
						for (Tuple<Object, Integer> t : attributeSaved.getValue()){
							
							//Value Found
							if (t.getX().equals(value)){
								valueFound = true;
								t.setY(t.getY() + modification); 
							}
						}
						
						//Never seen this value
						if (!valueFound)
							attributeSaved.getValue().add(new Tuple<Object, Integer>(value, modification));					
					}
					
				}
				
				//Never seen this attribute
				if (!attributeFound){
					List<Tuple<Object, Integer>> newList = new ArrayList<Tuple<Object, Integer>>();
					newList.add(new Tuple<Object, Integer>(value, modification));
					tuple.getY().put(attribute, newList);
				}
			}
		}
		
		//Never seen this vertex
		if (!vertexFound){
			List<Tuple<Object, Integer>> valuesList = new ArrayList<Tuple<Object, Integer>>();
			
			valuesList.add(new Tuple<Object, Integer>(value, modification));
			
			Map<String, List<Tuple<Object, Integer>>> attributesMap = new HashMap<String, List<Tuple<Object, Integer>>>();
			
			attributesMap.put(attribute, valuesList); 
					
			list.add(new Tuple<Long, Map<String, List<Tuple<Object, Integer>>>>(id, attributesMap));
		}
		
		return list;
	}
	
	public static List<Tuple<Long, Map<String, List<Tuple<Object, Integer>>>>> updateRemovedVertices(ImgVertex vertex){
		List<Tuple<Long, Map<String, List<Tuple<Object, Integer>>>>> modificationsSaved = new ArrayList<Tuple<Long, Map<String, List<Tuple<Object, Integer>>>>>();
		
		List<Long> oneHop = new ArrayList<Long>();
		List<Long> twoHop = new ArrayList<Long>();
		for(ImgEdge edge : vertex.getEdges()) {					
			oneHop.add(edge.getDestCellId());
			
			for (ImgEdge twoHopEdges : ((ImgVertex) ImgGraph.getInstance().retrieveCell(edge.getDestCellId())).getEdges()){
				if(twoHopEdges.getDestCellId() != vertex.getId()){
					twoHop.add(twoHopEdges.getDestCellId());
				}
			}
		}
		
		List<Long> idAlreadyDone = new ArrayList<Long>();
		List<Tuple<Long, Long>> undirectLinks = new ArrayList<Tuple<Long, Long>>();
		for(Long id : oneHop){
			idAlreadyDone.add(id);
			for(Long id25 : oneHop){
				if (!idAlreadyDone.contains(id25)){
					undirectLinks.add(new Tuple<Long, Long>(id, id25));
				}
			}
		}
		
		Map<String, List<Tuple<Object, Integer>>> vector = vertex.getNeighborhoodVector().getVector();
		Map<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndex = ImgGraph.getInstance().getAttributeIndex().getAttributeIndex();
		Map<String, Map<Object, List<Tuple<Long, Integer>>>> newAttributeIndex = ImgGraph.getInstance().getAttributeIndex().getAttributeIndex();
		
		for (Entry<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndexEntry : attributeIndex.entrySet()){
			
			//Check if there are values (Object) in this attribute (String) we need to remove
			if(vector.keySet().contains(attributeIndexEntry.getKey())){
				
				boolean attributeIndexEntryModified = false;
				List<Object> objectRemoved = new ArrayList<Object>();
				
				for (Entry<Object, List<Tuple<Long, Integer>>> attributeIndexEntryValue : attributeIndexEntry.getValue().entrySet()){
					
					//Check if this value (Object) is present in the removed vertex
					boolean present = false;
					for (Tuple<Object, Integer> tuple : vector.get(attributeIndexEntry.getKey())){
						if (tuple.getX().equals(attributeIndexEntryValue.getKey()))
							present = true;
					}
					
					
					boolean removed = false;
					List<Tuple<Long, Integer>> tupleRemoved = new ArrayList<Tuple<Long, Integer>>();
					if (present){
						//Search after Symmetric Direct Links
						for (String attribute : vertex.getAttributeKeys()){
							if (attribute.equals(attributeIndexEntry.getKey())){
								if (vertex.getAttribute(attribute).equals(attributeIndexEntryValue.getKey())){
									for (Tuple<Long, Integer> tuple : attributeIndexEntryValue.getValue()){
										if (oneHop.contains(tuple.getX())){
											tuple.setY(tuple.getY() - 50); //TODO
											modificationsSaved = updateList(tuple.getX(), attribute, attributeIndexEntryValue.getKey(), -50, modificationsSaved);
										}
										if (twoHop.contains(tuple.getX())){
											tuple.setY(tuple.getY() - 25); //TODO
											modificationsSaved = updateList(tuple.getX(), attribute, attributeIndexEntryValue.getKey(), -25, modificationsSaved);
										}
										if (tuple.getY() == 0)
											tupleRemoved.add(tuple);
									}
								}
								else{
									int i = 0;
									long firstId = 0;
									for (Tuple<Long, Integer> tuple : attributeIndexEntryValue.getValue()){
										if (i == 0){
											firstId = tuple.getX();
										}
										if (!tuple.getX().equals(vertex.getId())){
											for (Tuple<Long, Long> undirectLinksTuple : undirectLinks){
												if(undirectLinksTuple.getX().equals(firstId) && undirectLinksTuple.getY().equals(tuple.getX())){
													tuple.setY(tuple.getY() - 25); //TODO
													modificationsSaved = updateList(tuple.getX(), attribute, attributeIndexEntryValue.getKey(), -25, modificationsSaved);
												}
												else if(undirectLinksTuple.getY().equals(firstId) && undirectLinksTuple.getX().equals(tuple.getX())){
													tuple.setY(tuple.getY() - 25); //TODO
													modificationsSaved = updateList(tuple.getX(), attribute, attributeIndexEntryValue.getKey(), -25, modificationsSaved);	
												}
												if (tuple.getY() == 0)
													tupleRemoved.add(tuple);
											}
										}
										i++;
									}
								}
							}
						}

						//Search after possible tuple to remove
						for (Tuple<Long, Integer> tuple : attributeIndexEntryValue.getValue()){
							if (tuple.getX().equals(vertex.getId())){
								tupleRemoved.add(tuple); //TODO
								modificationsSaved = updateList(tuple.getX(), attributeIndexEntry.getKey(), attributeIndexEntryValue.getKey(), -tuple.getY(), modificationsSaved);
								removed = true;
							}
						}
					}		
					
					//Process these removals
					if (removed){
						attributeIndexEntryModified = true;
						for (Tuple<Long, Integer> tuple : tupleRemoved){
							attributeIndexEntryValue.getValue().remove(tuple);
						}
						if (attributeIndexEntryValue.getValue().size() == 0)
							objectRemoved.add(attributeIndexEntryValue.getKey());
					}
				}
				
				for (Object objectToRemove : objectRemoved)
					attributeIndexEntry.getValue().remove(objectToRemove);
				
				if (attributeIndexEntryModified){
					newAttributeIndex.put(attributeIndexEntry.getKey(), attributeIndexEntry.getValue());
				}
			}
		}
		ImgGraph.getInstance().getAttributeIndex().setAttributeIndex(newAttributeIndex);
		
		Map<Long, NeighborhoodVector> neighborhoodVectorMap = ImgGraph.getInstance().getNeighborhoodVectorMap();
		for (Tuple<Long, Map<String, List<Tuple<Object, Integer>>>> tuple : modificationsSaved){
			neighborhoodVectorMap.put(tuple.getX(), NeighborhoodVector.applyModification(((ImgVertex) CacheContainer.getCellCache().get(tuple.getX())).getNeighborhoodVector(), tuple.getY()));
		}
		
		//Garbage collector
		neighborhoodVectorMap.remove(vertex.getId());
		
		ImgGraph.getInstance().setNewNeighborhoodVectorMap(neighborhoodVectorMap);
		
		return modificationsSaved;
	}
	
	/**
	 * 
	 * @param vm1 newer values
	 * @param vm2 
	 * @return vm2 with the values of vm1 merged
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
