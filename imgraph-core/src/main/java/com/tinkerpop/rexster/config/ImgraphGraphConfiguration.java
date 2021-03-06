package com.tinkerpop.rexster.config;

import org.apache.commons.configuration.Configuration;
import org.infinispan.lifecycle.ComponentStatus;

import com.imgraph.networking.NodeServer;
import com.imgraph.storage.CacheContainer;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.imgraph.ImgraphGraph;

/**
 * @author Aldemar Reynaga
 * Implementation of the Rexster specification to start an Imgraph data server inside a Rexster server 
 */
public class ImgraphGraphConfiguration implements GraphConfiguration {

	@Override
	public Graph configureGraphInstance(Configuration properties)
			throws GraphConfigurationException {
		String configFile = properties.getString("config-file", null);
		
		try {
			
			System.setProperty("java.net.preferIPv4Stack" , "true");
		
			if (configFile != null)
				com.imgraph.common.Configuration.loadProperties(configFile);
			
			CacheContainer.getCacheContainer().start();
			CacheContainer.getCellCache().start();
			
			while (!CacheContainer.getCacheContainer().getStatus().equals(ComponentStatus.RUNNING));
			
			new Thread(new NodeServer()).start();
			
			Thread.sleep(2000);
			
			
		} catch (Exception x) {
			throw new GraphConfigurationException(x);
		}
		
		
		return ImgraphGraph.getInstance();
	}

}
