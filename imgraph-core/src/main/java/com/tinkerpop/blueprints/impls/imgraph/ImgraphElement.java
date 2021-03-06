package com.tinkerpop.blueprints.impls.imgraph;

import java.util.HashSet;
import java.util.Set;

import com.imgraph.model.Cell;
import com.tinkerpop.blueprints.Element;

/**
 * @author Aldemar Reynaga
 * Implementation of the Blueprints Element specification using the Cell class
 */
public abstract class ImgraphElement implements Element {

	protected final ImgraphGraph graph;
	protected Cell cell;
	
	public ImgraphElement(final ImgraphGraph graph) {
		this.graph = graph;
	}
	
	@Override
	public Object getId() {
		return cell.getId();
	}

	
	@SuppressWarnings("unchecked")
	public <T> T getProperty(final String key) {
		return (T) cell.getAttribute(key);
	}
	
	public void setProperty(String key, Object value) {
		//graph.autoStartTransaction();
		cell.putAttribute(key, value);
	}
	
	
	
	@SuppressWarnings("unchecked")
	public <T> T removeProperty(final String key) {
		return (T) cell.removeAttribute(key);
	}
	
	public Set<String> getPropertyKeys() {
		final Set<String> keys = new HashSet<String>();
        for (final String key : this.cell.getAttributeKeys()) {
            keys.add( key);
        }
        return keys;
	}
	

}
