package com.imgraph.networking.messages;

import java.util.ArrayList;
import java.util.List;

public class LocalVertexIdRepMsg extends Message {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6914506924928756246L;
	private List<Long> cellIds;
	
	public LocalVertexIdRepMsg() {
		super(MessageType.LOCAL_VERTEX_ID_REP);
		cellIds = new ArrayList<Long>();
	}

	public List<Long> getCellIds() {
		return cellIds;
	}

	public void setCellIds(List<Long> cellIds) {
		this.cellIds = cellIds;
	}
}
