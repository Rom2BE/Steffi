package com.imgraph.index;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.Cache;
import org.zeromq.ZMQ;

import com.imgraph.common.Configuration;
import com.imgraph.model.Cell;
import com.imgraph.model.ImgEdge;
import com.imgraph.model.ImgGraph;
import com.imgraph.model.ImgVertex;
import com.imgraph.networking.messages.LocalVectorUpdateRepMsg;
import com.imgraph.networking.messages.LocalVectorUpdateReqMsg;
import com.imgraph.networking.messages.Message;
import com.imgraph.storage.CacheContainer;
import com.imgraph.storage.StorageTools;

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
	
	public static void updateFullNeighborhoodVector(ImgVertex vertex){
		/**
		 * Get updated information
		 */
		List<Cell> cell1HopList = new ArrayList<Cell>();
		List<Cell> cell2HopList = new ArrayList<Cell>();
		List<Long> distantIds = new ArrayList<Long>();
		long id;
		if (vertex!=null){
			ImgVertex destVertex;
			ImgVertex dest2HVertex;
			
			Cache<Long, Cell> cache = CacheContainer.getCellCache();
			String localAddress = cache.getCacheManager().getAddress().toString();
			for (ImgEdge edge : vertex.getEdges()){ 					//Get 1 hop edges
				id = edge.getDestCellId();
				destVertex = (ImgVertex) cache.get(id);
				if (StorageTools.getCellAddress(id).equals(localAddress))
					destVertex.setNeighborhoodVector(updateNeighborhoodVector(destVertex));
				else
					distantIds.add(id);
				cell1HopList.add(destVertex);
								
				for (ImgEdge edge2H : destVertex.getEdges()){ 			//Get 2 hops edges
					if (edge2H.getDestCellId() != vertex.getId()) { 	//Do not go back on the original vertex
						id = edge2H.getDestCellId();
						dest2HVertex = (ImgVertex) cache.get(id);
						if (StorageTools.getCellAddress(id).equals(localAddress))
							dest2HVertex.setNeighborhoodVector(updateNeighborhoodVector(dest2HVertex));
						else
							distantIds.add(id);
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
				list.add(new Tuple<Object,Integer>(vertex.getAttribute(key), 100));
				vector.put(key, list);
			}
			//Add 1 Hop cells information
			vector = partialVectorUpdate(vector, cell1HopList, 50);
			
			//Add 2 Hops cells information
			vector = partialVectorUpdate(vector, cell2HopList, 25);
			
			vertex.setNeighborhoodVector(vector);
			
			/**
			 * Update Indexes
			 */
			Map<Long, NeighborhoodVector> neighborhoodVectorMap = ImgGraph.getInstance().getNeighborhoodVectorMap();
			neighborhoodVectorMap.put(vertex.getId(), new NeighborhoodVector(vector));
			//TODO remove old values when removing vertices
			//System.out.println("\nsetNeighborhoodVectorMap(neighborhoodVectorMap) in NeighborhoodVector");
			ImgGraph.getInstance().setNeighborhoodVectorMap(neighborhoodVectorMap);
			
			/**
			 * Update modified distant vertices
			 */
			Map<String, String> clusterAddresses = StorageTools.getAddressesIps();
			ZMQ.Socket socket = null;
			ZMQ.Context context = ImgGraph.getInstance().getZMQContext();
			
			try {
				for (Entry<String, String> entry : clusterAddresses.entrySet()) {
					//Only send this message to other machines
					if(!entry.getKey().equals(localAddress)){
						socket = context.socket(ZMQ.REQ);
						
						socket.connect("tcp://" + entry.getValue() + ":" + 
								Configuration.getProperty(Configuration.Key.NODE_PORT));
					
						LocalVectorUpdateReqMsg message = new LocalVectorUpdateReqMsg();
						
						message.setCellIds(distantIds);
						
						message.setUpdateType(false);
						
						message.setNeighborhoodVectorMap(neighborhoodVectorMap);
						
						socket.send(Message.convertMessageToBytes(message), 0);
						
						LocalVectorUpdateRepMsg response = (LocalVectorUpdateRepMsg) Message.readFromBytes(socket.recv(0));
						
						//System.out.println("\nsetNeighborhoodVectorMap(response.getNeighborhoodVectorMap()) in NeighborhoodVector");
						ImgGraph.getInstance().setNeighborhoodVectorMap(response.getNeighborhoodVectorMap());
						
						socket.close();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}finally {
				if (socket !=null)
					socket.close();
			}
		}	
		
		else
			System.out.println("Vertex is null");
		
	}

	
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
		
		/**
		 * Update Indexes
		 */
		Map<Long, NeighborhoodVector> neighborhoodVectorMap = ImgGraph.getInstance().getNeighborhoodVectorMap();
		neighborhoodVectorMap.put(vertex.getId(), new NeighborhoodVector(vector));
		//System.out.println("\nsetNeighborhoodVectorMap(neighborhoodVectorMap) in NeighborhoodVector");
		ImgGraph.getInstance().setNeighborhoodVectorMap(neighborhoodVectorMap);
		
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
	
	public static Map<Long, NeighborhoodVector> removeIds(Map<Long, NeighborhoodVector> neighborhoodVectorMap, List<Long> removedIds){
		for (Long id : removedIds){
			neighborhoodVectorMap.remove(id);
		}
		return neighborhoodVectorMap;
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