package com.imgraph.networking.messages;

import java.util.List;
import java.util.Map;

import com.imgraph.index.AttributeIndex;
import com.imgraph.index.NeighborhoodVector;

/**
 * @author Romain Capron
 * Asking for a list with the cells Ids locally stored in that machine
 */
public class LocalVectorUpdateRepMsg extends Message {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5730892153221681092L;
	private Map<Long, NeighborhoodVector> neighborhoodVectorMap;

	public Map<Long, NeighborhoodVector> getNeighborhoodVectorMap(){
		return neighborhoodVectorMap;
	}
	
	public void setNeighborhoodVectorMap(Map<Long, NeighborhoodVector> neighborhoodVectorMap){
		this.neighborhoodVectorMap = neighborhoodVectorMap;
	}

	public LocalVectorUpdateRepMsg() {
		super(MessageType.LOCAL_VECTOR_UPDATE_REP);
	}

	public LocalVectorUpdateRepMsg(String body) {
		super(MessageType.LOCAL_VECTOR_UPDATE_REP, body);
	}
}