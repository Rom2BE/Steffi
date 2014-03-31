package com.imgraph.networking.messages;

import java.util.List;
import java.util.Map;

import com.imgraph.index.AttributeIndex;
import com.imgraph.index.NeighborhoodVector;
import com.imgraph.index.Tuple;

/**
 * @author Romain Capron
 * //TODO
 */
public class LocalNeighborhoodVectorsRemovalUpdateReqMsg extends Message {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2327932523682870209L;
	private AttributeIndex attributeIndex;
	private Map<Long, NeighborhoodVector> neighborhoodVectorMap;
	private List<Tuple<Long, Map<String, List<Tuple<Object, Integer>>>>> modifications;

	public AttributeIndex getAttributeIndex() {
		return attributeIndex;
	}	

	public void setAttributeIndex(AttributeIndex attributeIndex) {
		this.attributeIndex = attributeIndex;
	}
	
	public Map<Long, NeighborhoodVector> getNeighborhoodVectorMap() {
		return neighborhoodVectorMap;
	}	

	public void setNeighborhoodVectorMap(Map<Long, NeighborhoodVector> neighborhoodVectorMap) {
		this.neighborhoodVectorMap = neighborhoodVectorMap;
	}
	
	public List<Tuple<Long, Map<String, List<Tuple<Object, Integer>>>>> getModifications() {
		return modifications;
	}	

	public void setModifications(List<Tuple<Long, Map<String, List<Tuple<Object, Integer>>>>> modifications) {
		this.modifications = modifications;
	}

	public LocalNeighborhoodVectorsRemovalUpdateReqMsg() {
		super(MessageType.LOCAL_NEIGHBORHOODVECTOR_REMOVAL_UPDATE_REQ);
	}

	public LocalNeighborhoodVectorsRemovalUpdateReqMsg(String body) {
		super(MessageType.LOCAL_NEIGHBORHOODVECTOR_REMOVAL_UPDATE_REQ, body);
	}
}