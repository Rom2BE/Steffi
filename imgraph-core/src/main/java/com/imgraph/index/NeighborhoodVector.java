package com.imgraph.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.imgraph.model.ImgEdge;
import com.imgraph.model.ImgVertex;
import com.tinkerpop.blueprints.impls.imgraph.ImgraphGraph;

public class NeighborhoodVector {

	Map<Pair<Object>, Float> vector;
	
	public NeighborhoodVector() {
		vector = new HashMap<Pair<Object>, Float>();
	}
	
	public NeighborhoodVector(List<Pair<Object>> list) {
		this();
		for (Pair<Object> pair : list){
			vector.put(pair, 1.0F);
		}
	}
	
	public Map<Pair<Object>, Float> getVector() {
		return vector;
	}
	
	public void setVector(Map<Pair<Object>, Float> vector) {
		this.vector = vector;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Map<Pair<Object>, Float> getNeighborhoodVector(long id){
		Map<Pair<Object>, Float> result = new HashMap<Pair<Object>, Float>();
		ImgVertex vertex = (ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(id);
		if (vertex!=null){
			for (String key : vertex.getAttributeKeys()){
				result.put(new Pair(key,vertex.getAttribute(key)), 1F); //The value stored in the original vertex 
			}
			ImgVertex destVertex;
			ImgVertex destVertex2H;
			for (ImgEdge edge : vertex.getEdges()){ 					//Get 1 hop edges 
				destVertex = (ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(edge.getDestCellId());
				for (String key : destVertex.getAttributeKeys()){
					//Values stored at 1 hop
					if (result.containsKey(key + " : " + destVertex.getAttribute(key)))	//Value already in the vector	
						result.put(new Pair(key,destVertex.getAttribute(key)), result.get(new Pair(key,destVertex.getAttribute(key)))+0.5F); 
					else													//Value seen for the first time
						result.put(new Pair(key,destVertex.getAttribute(key)), 0.5F);
				}
				for (ImgEdge edge2H : destVertex.getEdges()){ 			//Get 2 hops edges
					if (edge2H.getDestCellId() != vertex.getId()) { 	//Do not go back on the original vertex
						destVertex2H = (ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(edge2H.getDestCellId());
						for (String key : destVertex2H.getAttributeKeys()){
							//Values stored at 2 hops
							if (result.containsKey(key + " : " + destVertex2H.getAttribute(key)))	//Value already in the vector	
								result.put(new Pair(key,destVertex2H.getAttribute(key)), result.get(new Pair(key,destVertex2H.getAttribute(key)))+0.25F);
							else											//Value seen for the first time
								result.put(new Pair(key,destVertex2H.getAttribute(key)), 0.25F);
						}
					}
				}
			}
		}
		return result;
	}
	
	public String toString() {
		return "{"+","+"}";
	}
}
