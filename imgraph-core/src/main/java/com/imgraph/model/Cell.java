package com.imgraph.model;


import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;
import gnu.trove.procedure.TIntProcedure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;

import com.imgraph.storage.CellSequence;
import com.imgraph.storage.CellTransactionThread;



/**
 * @author Aldemar Reynaga
 * Root class of the graph data model representing a Cell
 */
public abstract class Cell implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 335721616065084854L;
	
	
	protected long id;
	protected Integer nameIndex;
	
	
	protected CellType cellType;
	
	protected TIntObjectHashMap<Object> attributes;
	
	
	public Cell(Long id, String name) {
		if (id == null)
			this.id = CellSequence.getNewCellId();
		else
			this.id = id;
		
		if (name != null) 
			this.nameIndex = ImgGraph.getInstance().getItemNameIndex(name);
	}
	
	protected TIntObjectMap<Object> getAndInitAttributes() {
		if (attributes == null)
			attributes = new TIntObjectHashMap<Object>();
		return attributes;
	}
	
	public void trimToSize() {
		if (attributes != null)
			attributes.compact();
	}
	
	protected int getKeyIndex(String key) {
		return ImgGraph.getInstance().getItemNameIndex(key);
	}
	
	public TIntObjectMap<Object> getAttributes() {
		return attributes;
	}
	
	public void putAttributes(TIntObjectMap<Object> attributes) {
		getAndInitAttributes().putAll(attributes);
	}
	
	public void putAttribute(String key, Object value) {
		int keyIndex = getKeyIndex(key);
		Object oldValue = getAndInitAttributes().get(keyIndex); 
		getAndInitAttributes().put(keyIndex, value);
		CellTransactionThread.get().setCellProperty(this, keyIndex, value, oldValue);
	}
	
	public Object removeAttribute(String key) {
		Object value = null;
		int keyIndex = getKeyIndex(key);
		if (attributes != null) {
			value = attributes.remove(keyIndex);
		}
		
		CellTransactionThread.get().removeCellProperty(this, keyIndex);
		return value;
	}
	
	public Object getAttribute(String key) {
		if (attributes != null)
			return attributes.get(getKeyIndex(key));
		return null;
	}
	
	public void clearAttributes() {
		if (attributes != null)
			attributes.clear();
	}
	
	public Iterable<String> getAttributeKeys() {
		return new Iterable<String>() {
				
			@SuppressWarnings("unchecked")
			@Override
			public Iterator<String> iterator() {
				if (attributes != null) {
					final List<String> keys = new ArrayList<String>();
					final ImgGraph graph = ImgGraph.getInstance();
					
					attributes.forEachKey(new TIntProcedure() {
						@Override
						public boolean execute(int keyIndex) {
							keys.add(graph.getItemName(keyIndex));
							return true;
						}
					});
					
					return keys.iterator();
				} else
					return IteratorUtils.emptyIterator();
			}
		};	
	}
	
	public long getId() {
		return id;
	}
	
	public String getName() {
		if (nameIndex != null)
			return ImgGraph.getInstance().getItemName(nameIndex);
		return null;
	}
	
	protected void setName(String name) {
		if (name != null) 
			nameIndex = ImgGraph.getInstance().getItemNameIndex(name);	
	}
	
	public CellType getCellType() {
		return cellType;
	}

	@Override
	public String toString() {
		final StringBuffer string = new StringBuffer("");
		string.append(getCellType() +  ":: ID: " + getId() + ", NAME: " + getName());
		
		
		if (attributes != null && !attributes.isEmpty()) {
			string.append("\n\tATTRIBUTES:");
			final ImgGraph graph = ImgGraph.getInstance();
			
			attributes.forEachEntry(new TIntObjectProcedure<Object>() {

				@Override
				public boolean execute(int keyIndex, Object value) {
					string.append("\n\tKEY: " + graph.getItemName(keyIndex) + " VALUE: " + value);
					return true;
				}
			});
		}
		return string.toString();
	}
	
	public Cell clone() {
		Cell cell = null;
		switch (this.cellType) {
		case EDGE:
			cell = new ExtImgEdge(this.id, this.getName());
			break;
		case HYPEREDGE:
			break;
		case VERTEX:
			cell = new ImgVertex(this.id, this.getName(), false);
		default:
			break;
		}
		
		if (this.attributes != null) 
			cell.attributes = new TIntObjectHashMap<Object>(this.attributes);
		
		return cell;
	}
}
