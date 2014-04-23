package com.imgraph.index.actors;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.Cache;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

import com.imgraph.index.AttributeIndex;
import com.imgraph.index.NeighborhoodVector;
import com.imgraph.index.Tuple;
import com.imgraph.model.Cell;
import com.imgraph.model.ImgVertex;
import com.imgraph.networking.messages.IndexUpdateReqMsg;
import com.imgraph.storage.CacheContainer;
import com.imgraph.storage.StorageTools;

/**
 * @author Romain Capron
 * Akka actor started when a vertex has been modified and modifications need to be sent to other machines
 */
public class IndexUpdateActor extends UntypedActor{

	@SuppressWarnings("unchecked")
	@Override
	public void onReceive(Object message) throws Exception {
		if(message instanceof Tuple) {
			IndexUpdateReqMsg requestMessage = new IndexUpdateReqMsg();

			requestMessage.setCellIds(((Tuple<List<Long>, Map<Long, Map<String, List<Tuple<Object, Integer>>>>>) message).getX());

			requestMessage.setModificationsNeeded(((Tuple<List<Long>, Map<Long, Map<String, List<Tuple<Object, Integer>>>>>) message).getY());
			
			Map<String, String> clusterAddresses = StorageTools.getAddressesIps();
			for (Entry<String, String> entry : clusterAddresses.entrySet()) {
				//Only send this message to other machines
				if(!entry.getKey().equals(CacheContainer.getCellCache().getCacheManager().getAddress().toString())){
					ActorRef dest = getContext().actorFor(
							"akka://IndexUpdateDaemon@"
									+ entry.getValue()
									+ ":5678/user/indexUpdateActor");
					
					dest.tell(requestMessage, getSelf());
				}
			}
		} else if (message instanceof IndexUpdateReqMsg){
			Map<Long, Map<String, List<Tuple<Object, Integer>>>> modificationsNeeded = ((IndexUpdateReqMsg) message).getModificationsNeeded();
			//Update Neighborhood vectors of local vertices (if modified)
			Cache<Long, Cell> cache = CacheContainer.getCellCache();
			for (Long cellId : ((IndexUpdateReqMsg) message).getCellIds()){
				if (StorageTools.getCellAddress(cellId).equals(cache.getCacheManager().getAddress().toString())){
					//Vertex stored on this machine
					ImgVertex vertex = (ImgVertex) cache.get(cellId);
					if (vertex != null)
						vertex.setNeighborhoodVector(NeighborhoodVector.applyModifications(vertex.getNeighborhoodVector(), modificationsNeeded.get(cellId)));
				}
			}

			//Update Attribute Index
			AttributeIndex.applyModifications(modificationsNeeded);
		}
		else
			System.out.println("Wrong instance of message received");
	}
}