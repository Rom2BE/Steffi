package com.imgraph.networking.messages;


public class LocalVertexIdReqMsg extends Message {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -785797581894615636L;

	public LocalVertexIdReqMsg() {
		super(MessageType.LOCAL_VERTEX_ID_REQ);
	}
}
