package com.imgraph.networking.messages;

import java.util.List;
import java.util.Map;

import com.imgraph.index.Tuple;

/**
 * @author Romain Capron
 * Message sent to distant machines in order to apply index modifications.
 */
public class IndexUpdateReqMsg extends Message {
	
	private static final long serialVersionUID = 5729073213874926570L;
	private List<Long> cellIds;
	Map<Long, Map<String, List<Tuple<Object, Integer>>>> modificationsNeeded;
	
	public List<Long> getCellIds() {
		return cellIds;
	}

	
	public void setCellIds(List<Long> cellIds) {
		this.cellIds = cellIds;
	}
	
	
	public Map<Long, Map<String, List<Tuple<Object, Integer>>>> getModificationsNeeded(){
		return modificationsNeeded;
	}
	
	
	public void setModificationsNeeded(Map<Long, Map<String, List<Tuple<Object, Integer>>>> modificationsNeeded){
		this.modificationsNeeded = modificationsNeeded;
	}
	
	
	public IndexUpdateReqMsg() {
		super(MessageType.INDEX_UPDATE_REQ);
	}

	
	public IndexUpdateReqMsg(String body) {
		super(MessageType.INDEX_UPDATE_REQ, body);
	}
}