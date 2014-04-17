package com.imgraph.index;

import gnu.trove.procedure.TIntObjectProcedure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
	
	
	public AttributeIndex(
			Map<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndex) {
		this.attributeIndex = attributeIndex;
	}
	
	
	public Map<String, Map<Object, List<Tuple<Long, Integer>>>> getAttributeIndex(){
		return attributeIndex;
	}
	
	
	public void setAttributeIndex(
			Map<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndex){
		this.attributeIndex = attributeIndex;
	}

	/**
	 * TODO
	 * @param id
	 * @param addedKeys
	 * @param removedKeys
	 * @param changedEntries
	 * @param addedEdges
	 * @param removedEdges
	 * @param modificationsNeeded
	 * @return
	 */
	public static Map<Long, Map<String, List<Tuple<Object, Integer>>>> getAttributeIndexModifications(
			Long id,
			Set<String> addedKeys,
			Set<Tuple<String, Object>> removedKeys,
			Set<Tuple<String, Tuple<Object,Object>>> changedEntries,
			List<Long> addedEdges, 
			List<Long> removedEdges, 
			Map<Long, Map<String, List<Tuple<Object, Integer>>>> modificationsNeeded) {

		/*
		 * Get only existing neighbors
		 */
		List<Long> oneHop = new ArrayList<Long>();
		List<Long> twoHop = new ArrayList<Long>(); //Can contain several time the same id if there are several paths.
		
		for(ImgEdge edge : ((ImgVertex) CacheContainer.getCellCache().get(id)).getEdges()) {	
			
			ImgVertex destVertex = (ImgVertex) CacheContainer.getCellCache().get(edge.getDestCellId());
			if (destVertex != null){
				oneHop.add(edge.getDestCellId());
				
				for (ImgEdge twoHopEdges : destVertex.getEdges()){
					if(twoHopEdges.getDestCellId() != id){
						twoHop.add(twoHopEdges.getDestCellId());
					}
				}
			}
		}
		
		/*
		 * BEFORE
		 *
		System.out.println("\nVertex : " + id);
		System.out.println("addedKeys : " + addedKeys);
		System.out.println("removedKeys : " + removedKeys);
		System.out.println("changedEntries : " + changedEntries);
		System.out.println("One-hop : " + oneHop);
		System.out.println("Two-hop : " + twoHop);
		System.out.println("addedEdges : " + addedEdges);
		System.out.println("removedEdges : " + removedEdges); 
		*/
		/*
		 * A) New attribute A1 with a value V1 -> stored in addedKeys
		 * Its neighbors are maybe not created yet! Will be adjusted at D.
		 */
		if (addedKeys != null && changedEntries != null){
			for (String attribute : addedKeys){
				for (Tuple<String, Tuple<Object,Object>> entry : changedEntries){
					if (entry.getX().equals(attribute)){
						//Modified Vertex : +100
						modificationsNeeded = getModificationsNeeded(
								new ArrayList<Long>(Arrays.asList(id)), 
								modificationsNeeded, 
								attribute, 
								entry.getY().getY(), 
								100);
						//One-hop neighbors : +50
						modificationsNeeded = getModificationsNeeded(
								oneHop, 
								modificationsNeeded, 
								attribute, 
								entry.getY().getY(), 
								50);
						//Two-hops neighbors : +25
						modificationsNeeded = getModificationsNeeded(
								twoHop, 
								modificationsNeeded, 
								attribute, 
								entry.getY().getY(), 
								25);
					}
				}
			}
		}
		
		/*
		 * B) Modify the value V1 to V2 of the attribute A1 -> stored in changedEntries
		 */
		if (addedKeys != null && changedEntries != null){
			for (Tuple<String, Tuple<Object,Object>> entry : changedEntries){
				//Tuple<Attribute, Tuple<Old Value,New Value>>
				if (!addedKeys.contains(entry.getX())){
					//Remove previous value
					//Modified Vertex : -100
					modificationsNeeded = getModificationsNeeded(
							new ArrayList<Long>(Arrays.asList(id)), 
							modificationsNeeded, 
							entry.getX(), 
							entry.getY().getX(), 
							-100);
					//One-hop neighbors : -50
					modificationsNeeded = getModificationsNeeded(
							oneHop, 
							modificationsNeeded, 
							entry.getX(), 
							entry.getY().getX(), 
							-50);
					//Two-hops neighbors : -25
					modificationsNeeded = getModificationsNeeded(
							twoHop, 
							modificationsNeeded, 
							entry.getX(), 
							entry.getY().getX(), 
							-25);
					
					//Propagate the new value
					//Modified Vertex : +100
					modificationsNeeded = getModificationsNeeded(
							new ArrayList<Long>(Arrays.asList(id)), 
							modificationsNeeded, 
							entry.getX(), 
							entry.getY().getY(), 
							100);
					//One-hop neighbors : +50
					modificationsNeeded = getModificationsNeeded(
							oneHop, 
							modificationsNeeded, 
							entry.getX(), 
							entry.getY().getY(), 
							50);
					//Two-hops neighbors : +25
					modificationsNeeded = getModificationsNeeded(
							twoHop, 
							modificationsNeeded, 
							entry.getX(), 
							entry.getY().getY(), 
							25);
				}
			}
		}
		/*
		 * C) Remove an existing attribute A1 with all its values V1,...,Vi -> stored in removedKeys
		 */
		if (removedKeys != null){
			for (Tuple<String, Object> attributeRemoved : removedKeys){
				//Remove previous value
				//Modified Vertex : -100
				modificationsNeeded = getModificationsNeeded(
						new ArrayList<Long>(Arrays.asList(id)), 
						modificationsNeeded, 
						attributeRemoved.getX(), 
						attributeRemoved.getY(), 
						-100);
				//One-hop neighbors : -50
				modificationsNeeded = getModificationsNeeded(
						oneHop, 
						modificationsNeeded, 
						attributeRemoved.getX(), 
						attributeRemoved.getY(), 
						-50);
				//Two-hops neighbors : -25
				modificationsNeeded = getModificationsNeeded(
						twoHop, 
						modificationsNeeded, 
						attributeRemoved.getX(), 
						attributeRemoved.getY(), 
						-25);
			}
		}
		/*
		 * D) Add a new edges between this vertex and its new neighbors (that already exists)
		 */
		if (addedEdges != null){
			//One-hop propagation
			for (Long newDestId : oneHop){
				final Map<String, Object> attributeMap = new HashMap<String, Object>(); 
				if (addedEdges.contains(newDestId)){
					ImgVertex destVertex = (ImgVertex) CacheContainer.getCellCache().get(newDestId);
					final ImgGraph graph = ImgGraph.getInstance();
					
					if (destVertex.getAttributes() != null && !destVertex.getAttributes().isEmpty()) {
						destVertex.getAttributes().forEachEntry(new TIntObjectProcedure<Object>() {
		
							@Override
							public boolean execute(int keyIndex, Object value) {
								attributeMap.put(graph.getItemName(keyIndex), value);
								return true;
							}
						});
						
						List<Long> nowReachable = new ArrayList<Long>(oneHop);
						nowReachable.remove(newDestId);
						
						for (Entry<String, Object> entry : attributeMap.entrySet()){
							//Modified Vertex : +50
							modificationsNeeded = getModificationsNeeded(
									new ArrayList<Long>(Arrays.asList(id)), 
									modificationsNeeded, 
									entry.getKey(), 
									entry.getValue(), 
									50);
							//One-hop neighbors : +25
							modificationsNeeded = getModificationsNeeded(
									nowReachable, 
									modificationsNeeded, 
									entry.getKey(), 
									entry.getValue(), 
									25);
						}
					}
				}
			}
			
			//Two-hop propagation
			List<Long> alreadyDone = new ArrayList<Long>();
			for (Long newDestId : twoHop){
				final Map<String, Object> attributeMap = new HashMap<String, Object>();
				ImgVertex destVertex = (ImgVertex) CacheContainer.getCellCache().get(newDestId);
				
				if (destVertex != null && !alreadyDone.contains(newDestId)){
					alreadyDone.add(newDestId);
					boolean useNewEdge = false;
					for (ImgEdge edge : destVertex.getEdges()){
						if (addedEdges.contains(edge.getDestCellId()))
							useNewEdge = true;
					}
					
					if (useNewEdge){
						final ImgGraph graph = ImgGraph.getInstance();
						if (destVertex.getAttributes() != null && !destVertex.getAttributes().isEmpty()) {
							destVertex.getAttributes().forEachEntry(new TIntObjectProcedure<Object>() {
								@Override
								public boolean execute(int keyIndex, Object value) {
									attributeMap.put(graph.getItemName(keyIndex), value);
									return true;
								}
							});
							
							List<Long> nowReachable = new ArrayList<Long>(oneHop);
							nowReachable.remove(newDestId);
							for (Entry<String, Object> entry : attributeMap.entrySet()){
								//Modified Vertex : +25
								modificationsNeeded = getModificationsNeeded(
										new ArrayList<Long>(Arrays.asList(id)), 
										modificationsNeeded, 
										entry.getKey(), 
										entry.getValue(), 
										25);
							}
						}
					}
				}
			}
		}
		
		/*
		 * E) 1- Remove values that have been propagated in the removed Cell
		 *    2- Remove propagated values to this removed vertex
		 */
		if (removedKeys != null && addedKeys == null){
			//1- Remove values that have been propagated in the removed Cell
			ImgVertex vertex = (ImgVertex) CacheContainer.getCellCache().get(id);
			for (Entry<String, List<Tuple<Object, Integer>>> entry : vertex.getNeighborhoodVector().getVector().entrySet()){
				for (Tuple<Object, Integer> tupleInNV : entry.getValue()){
					//if not removed, need to do it now
					boolean present = false;
					for (Tuple<String, Object> tupleInRemovedKeys : removedKeys){
						if (entry.getKey().equals(tupleInRemovedKeys.getX()) && tupleInNV.getX().equals(tupleInRemovedKeys.getY()))
							present = true;
					}
					if (!present){
						modificationsNeeded = getModificationsNeeded(
								new ArrayList<Long>(Arrays.asList(id)), 
								modificationsNeeded, 
								entry.getKey(), 
								tupleInNV.getX(), 
								-tupleInNV.getY());
					}
				}
			}
			//2- Remove propagated values to this removed vertex
			//Save {attribute, value} stored in one-hop neighbors
			Map<Long, Map<String, Object>> savedMap = new HashMap<Long, Map<String, Object>>();
			for (Long oneHopId : oneHop){
				//Get attributes stored in this vertex
				final Map<String, Object> attributeMap = new HashMap<String, Object>(); 
				ImgVertex neighbor = (ImgVertex) CacheContainer.getCellCache().get(oneHopId);
				if (neighbor != null){
					//Check attribute modifications
					if (neighbor.getAttributes() != null && !neighbor.getAttributes().isEmpty()) {
						final ImgGraph graph = ImgGraph.getInstance();
						
						neighbor.getAttributes().forEachEntry(new TIntObjectProcedure<Object>() {

							@Override
							public boolean execute(int keyIndex, Object value) {
								attributeMap.put(graph.getItemName(keyIndex), value);
								return true;
							}
						});
					}
				}
				savedMap.put(oneHopId, attributeMap);
			}
			
			for (Entry<Long, Map<String, Object>> entry : savedMap.entrySet()){
				for (Long oneHopId : oneHop){
					if (!entry.getKey().equals(oneHopId)){
						for (Entry<String, Object> attributeValueToRemove : entry.getValue().entrySet()){
							modificationsNeeded = getModificationsNeeded(
									new ArrayList<Long>(Arrays.asList(oneHopId)), 
									modificationsNeeded, 
									attributeValueToRemove.getKey(), 
									attributeValueToRemove.getValue(), 
									-25);
						}
					}
				}
			}
		}
		
		//TODO remove edge when no vertex removed
		
		return modificationsNeeded;
	}

	/**
	 * TODO
	 * @param idList
	 * @param modificationsNeeded
	 * @param attribute
	 * @param value
	 * @param intensity
	 * @return
	 */
	private static Map<Long, Map<String, List<Tuple<Object, Integer>>>> getModificationsNeeded(
			List<Long> idList,
			Map<Long, Map<String, List<Tuple<Object, Integer>>>> modificationsNeeded,
			String attribute, 
			Object value,
			int intensity) {
		
		for (Long id : idList){
			Tuple<Object, Integer> tuple = null;
			List<Tuple<Object, Integer>> list = null;
			Map<String, List<Tuple<Object, Integer>>> map = null;
			
			//Check if this vertex has pending modifications
			if (modificationsNeeded.get(id) != null){
				Map<String, List<Tuple<Object, Integer>>> attributePendingModifications = modificationsNeeded.get(id);
				//Check if this vertex has pending modifications for this attribute
				if (attributePendingModifications.get(attribute) != null){
					list = attributePendingModifications.get(attribute);
					boolean present = false;
					//Check if this vertex has pending modifications for this attribute value
					for (Tuple<Object, Integer> pendingAttributeValue : list){
						if (pendingAttributeValue.getX().equals(value)){
							present = true;
							pendingAttributeValue.setY(pendingAttributeValue.getY() + intensity);
						}
					}
					//First modification for this attribute value
					if (!present){
						tuple = new Tuple<Object, Integer>(value, intensity);
						list.add(tuple);
					}
				}
				//First modification for this attribute
				else {
					tuple = new Tuple<Object, Integer>(value, intensity);
					list = new ArrayList<Tuple<Object, Integer>>();
					list.add(tuple);
				}
				
				map = modificationsNeeded.get(id);
			}
			//First modification for this vertex
			else {
				tuple = new Tuple<Object, Integer>(value, intensity);
				list = new ArrayList<Tuple<Object, Integer>>();
				list.add(tuple);
				map = new HashMap<String, List<Tuple<Object, Integer>>>();
			}
	
			map.put(attribute, list);
			modificationsNeeded.put(id, map);
		}
		
		return modificationsNeeded;
	}
	
	/**
	 * TODO
	 * @param modificationsNeeded
	 */
	public static void applyModifications(Map<Long, Map<String, List<Tuple<Object, Integer>>>> modificationsNeeded) {
		Map<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndex = ImgGraph.getInstance().getAttributeIndex().getAttributeIndex();
		
		for (Entry<Long, Map<String, List<Tuple<Object, Integer>>>> vertexModifications : modificationsNeeded.entrySet()){
			for (Entry<String, List<Tuple<Object, Integer>>> attributeModifications : vertexModifications.getValue().entrySet()){
				Map<Object, List<Tuple<Long, Integer>>> attributeValuesIndexed = attributeIndex.get(attributeModifications.getKey());
				
				for (Tuple<Object, Integer> valuesModifications : attributeModifications.getValue()){
					/*
					 * Modify the attribute
					 */
					if (attributeValuesIndexed != null){
						/*
						 * Modify its values
						 */
						List<Tuple<Long, Integer>> valuesIndexed = attributeValuesIndexed.get(valuesModifications.getX());
						if (valuesIndexed != null){
							/*
							 * Modify an existing one
							 */
							boolean present = false;
							List<Tuple<Long, Integer>> tupleToRemove = new ArrayList<Tuple<Long, Integer>>();
							for (Tuple<Long, Integer> vertexIndexed : valuesIndexed){
								if (vertexIndexed.getX().equals(vertexModifications.getKey())){
									present = true;
									vertexIndexed.setY(vertexIndexed.getY() + valuesModifications.getY());
								}
								if (vertexIndexed.getY() == 0){
									tupleToRemove.add(vertexIndexed);
								}
							}
							for (Tuple<Long, Integer> tuple : tupleToRemove){
								valuesIndexed.remove(tuple);
							}
							/*
							 * Create a new one
							 */
							if (!present){
								valuesIndexed.add(new Tuple<Long, Integer>(vertexModifications.getKey(), valuesModifications.getY()));
							}
						}
						/*
						 * Value seen for the first time
						 */
						else {
							valuesIndexed = new ArrayList<Tuple<Long, Integer>>();
							valuesIndexed.add(new Tuple<Long, Integer>(vertexModifications.getKey(), valuesModifications.getY()));
						}

						/*
						 * This value has been removed
						 */
						if (valuesIndexed.size() == 0)
							attributeValuesIndexed.remove(valuesModifications.getX());
						else
							attributeValuesIndexed.put(valuesModifications.getX(), valuesIndexed);
							
						/*
						 * This attribute has been removed
						 */
						if (attributeValuesIndexed.size() == 0)
							attributeIndex.remove(attributeModifications.getKey());
						else
							attributeIndex.put(attributeModifications.getKey(), attributeValuesIndexed);
					}
					/*
					 * Attribute seen for the first time
					 */
					else {
						List<Tuple<Long, Integer>> valuesIndexed = new ArrayList<Tuple<Long, Integer>>();
						valuesIndexed.add(new Tuple<Long, Integer>(vertexModifications.getKey(), valuesModifications.getY()));
						attributeValuesIndexed = new HashMap<Object, List<Tuple<Long, Integer>>>();
						attributeValuesIndexed.put(valuesModifications.getX(), valuesIndexed);
						attributeIndex.put(attributeModifications.getKey(), attributeValuesIndexed);
					}
				}
			}
		}
		ImgGraph.getInstance().setAttributeIndex(new AttributeIndex(attributeIndex));
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