package com.imgraph.networking.messages;

import java.util.List;

/**
 * @author Romain Capron
 * Asking for a list with the cells Ids locally stored in that machine
 */
public class LocalVectorUpdateReqMsg extends Message {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5415157323985027401L;
	private List<Long> cellIds;
	private boolean updateType; //True for full update, false for local update

	public List<Long> getCellIds() {
		return cellIds;
	}

	public void setCellIds(List<Long> cellIds) {
		this.cellIds = cellIds;
	}
	
	public boolean getUpdateType() {
		return updateType;
	}

	public void setUpdateType(boolean updateType) {
		this.updateType = updateType;
	}

	public LocalVectorUpdateReqMsg() {
		super(MessageType.LOCAL_VECTOR_UPDATE_REQ);
	}

	public LocalVectorUpdateReqMsg(String body) {
		super(MessageType.LOCAL_VECTOR_UPDATE_REQ, body);
	}
}