package com.imgraph.networking.messages;


import java.io.Serializable;
import java.util.List;

import com.imgraph.loader.LoadVertexInfo;

public class LoadMessage extends Message implements Serializable {


	public enum LoadFileType{
		IMGP_FILE,
		SIMPLE_TEXT_FILE,
		ADJ_LIST_TEXT_FILE;

	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 2424152852993550400L;
	private int loaderIndex;
	private List<LoadVertexInfo> verticesInfo;
	private LoadFileType loadFileType;

	public LoadMessage() {
		super(MessageType.LOAD_REQ);
	}

	public List<LoadVertexInfo> getVerticesInfo() {
		return verticesInfo;
	}

	public void setVerticesInfo(List<LoadVertexInfo> verticesInfo) {
		this.verticesInfo = verticesInfo;
	}

	public LoadFileType getLoadFileType() {
		return loadFileType;
	}

	public void setLoadFileType(LoadFileType loadFileType) {
		this.loadFileType = loadFileType;
	}

	@Override
	public String toString() {
		return super.toString() + " FILE_TYPE: " + loadFileType + " #VERT_INFO: " + 
				this.verticesInfo;
	}

	public int getLoaderIndex() {
		return loaderIndex;
	}

	public void setLoaderIndex(int loaderIndex) {
		this.loaderIndex = loaderIndex;
	}




}
