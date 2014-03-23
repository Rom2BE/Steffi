package com.imgraph.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.imgraph.model.Cell;
import com.imgraph.model.ImgEdge;
import com.imgraph.model.ImgVertex;
import com.imgraph.storage.CacheContainer;

public class NeighborhoodVector implements Serializable{

	
	/**
	 * 
	 */
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
	
	public static void updateFullNeighborhoodVector(ImgVertex vertex){
		/**
		 * Get updated information
		 */
		List<Cell> cell1HopList = new ArrayList<Cell>();
		List<Cell> cell2HopList = new ArrayList<Cell>();
		if (vertex!=null){
			ImgVertex destVertex;
			ImgVertex dest2HVertex;
			for (ImgEdge edge : vertex.getEdges()){ 					//Get 1 hop edges
				destVertex = (ImgVertex) CacheContainer.getCellCache().get(edge.getDestCellId());
				destVertex.setNeighborhoodVector(updateNeighborhoodVector(destVertex));
				cell1HopList.add(destVertex);
								
				for (ImgEdge edge2H : destVertex.getEdges()){ 			//Get 2 hops edges
					if (edge2H.getDestCellId() != vertex.getId()) { 	//Do not go back on the original vertex
						dest2HVertex = (ImgVertex) CacheContainer.getCellCache().get(edge2H.getDestCellId());
						dest2HVertex.setNeighborhoodVector(updateNeighborhoodVector(dest2HVertex));
						cell2HopList.add(dest2HVertex);
					}
				}
			}
			/**
			 * Create an updated vector
			 */
			Map<String, List<Tuple<Object,Integer>>> vector = new HashMap<String, List<Tuple<Object,Integer>>>();
			
			//Add original cell information
			for (String key : vertex.getAttributeKeys()){
				List<Tuple<Object,Integer>> list = new ArrayList<Tuple<Object,Integer>>();
				//TODO
				list.add(new Tuple<Object,Integer>(vertex.getAttribute(key), 100));
				vector.put(key, list);
			}
			//Add 1 Hop cells information
			vector = partialVectorUpdate(vector, cell1HopList, 50);
			
			//Add 2 Hops cells information
			vector = partialVectorUpdate(vector, cell2HopList, 25);
			
			vertex.setNeighborhoodVector(vector);
		}	
		
		else
			System.out.println("Vertex is null");
		
	}
	//TODO one method
	public static Map<String, List<Tuple<Object,Integer>>> updateNeighborhoodVector(ImgVertex vertex){
		/**
		 * Get updated information
		 */
		List<Cell> cell1HopList = new ArrayList<Cell>();
		List<Cell> cell2HopList = new ArrayList<Cell>();
		if (vertex!=null){
			ImgVertex destVertex;
			ImgVertex dest2HVertex;
			for (ImgEdge edge : vertex.getEdges()){ 					//Get 1 hop edges
				destVertex = (ImgVertex) CacheContainer.getCellCache().get(edge.getDestCellId());
				cell1HopList.add(destVertex);

				for (ImgEdge edge2H : destVertex.getEdges()){ 			//Get 2 hops edges
					if (edge2H.getDestCellId() != vertex.getId()) { 	//Do not go back on the original vertex
						dest2HVertex = (ImgVertex) CacheContainer.getCellCache().get(edge2H.getDestCellId());
						cell2HopList.add(dest2HVertex);
					}
				}
			}
		}	
		
		
		/**
		 * Create an updated vector
		 */
		Map<String, List<Tuple<Object,Integer>>> vector = new HashMap<String, List<Tuple<Object,Integer>>>();
		
		//Add original cell information
		for (String key : vertex.getAttributeKeys()){
			List<Tuple<Object,Integer>> list = new ArrayList<Tuple<Object,Integer>>();
			list.add(new Tuple<Object,Integer>(vertex.getAttribute(key), 100));
			vector.put(key, list);
		}
		//Add 1 Hop cells information
		vector = partialVectorUpdate(vector, cell1HopList, 50);
		
		//Add 2 Hops cells information
		vector = partialVectorUpdate(vector, cell2HopList, 25);
		
		return vector;
	}

	private static Map<String, List<Tuple<Object, Integer>>> partialVectorUpdate(
			Map<String, List<Tuple<Object, Integer>>> vector,
			List<Cell> cellList,
			int value) {
		boolean present;
		List<Tuple<Object,Integer>> list;
		for (Cell cell : cellList){
			for (String key : cell.getAttributeKeys()){
				//Key already seen
				if(vector.containsKey(key)){
					present = false;
					list = vector.get(key);
					for (Tuple<Object,Integer> tuple : vector.get(key)){
						//Attribute already seen
						if(tuple.getX().equals(cell.getAttribute(key))){
							present = true;
							tuple.setY(tuple.getY() + value);
						}
					}
					//Attribute never seen
					if (!present){
						list.add(new Tuple<Object,Integer>(cell.getAttribute(key), value));
						vector.put(key, list);
					}
				}
				//Key never seen
				else{
					//TODO
				}
			}
		}
		return vector;
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