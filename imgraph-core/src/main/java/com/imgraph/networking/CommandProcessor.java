package com.imgraph.networking;

import gnu.trove.procedure.TLongProcedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.Cache;
import org.zeromq.ZMQ.Socket;

import com.imgraph.model.Cell;
import com.imgraph.model.CellType;
import com.imgraph.model.ImgGraph;
import com.imgraph.model.ImgIndexedEdges;
import com.imgraph.model.ImgVertex;
import com.imgraph.networking.messages.AddressVertexRepMsg;
import com.imgraph.networking.messages.AddressVertexReqMsg;
import com.imgraph.networking.messages.ClusterAddressesRep;
import com.imgraph.networking.messages.IdentifiableMessage;
import com.imgraph.networking.messages.LocalNeighborsRepMsg;
import com.imgraph.networking.messages.LocalNeighborsReqMsg;
import com.imgraph.networking.messages.LocalVertexIdRepMsg;
import com.imgraph.networking.messages.LocalVertexIdReqMsg;
import com.imgraph.networking.messages.Message;
import com.imgraph.networking.messages.MessageType;
import com.imgraph.networking.messages.Update2HNReqMsg;
import com.imgraph.networking.messages.WriteFileReqMsg;
import com.imgraph.storage.CacheContainer;
import com.imgraph.storage.ImgpFileTools;
import com.imgraph.storage.Local2HopNeighborProcessor;
import com.imgraph.storage.Local2HopNeighborUpdater;
import com.imgraph.storage.StorageTools;
import com.imgraph.testing.TestTools;
import com.tinkerpop.blueprints.impls.imgraph.ImgraphGraph;

/**
 * @author Aldemar Reynaga
 * Contains the functions called on the arrival of command request messages
 */
public abstract class CommandProcessor {
	
	
	
	public static void processLocal2HRequest(Socket socket, final LocalNeighborsReqMsg reqMsg) throws IOException {
		final LocalNeighborsRepMsg repMsg = new LocalNeighborsRepMsg();
		
		reqMsg.getVertexIds().forEach(new TLongProcedure() {
			
			@Override
			public boolean execute(long vertexId) {
				ImgGraph graph = ImgGraph.getInstance();  
				ImgIndexedEdges edgeMap = ((ImgVertex)graph.retrieveRawCell(vertexId)).getEdgeMapByAddress(reqMsg.getLocalAddress());
				
				if (edgeMap != null && edgeMap.hasMoreThanOneEdge())
					repMsg.getVertexEdgeMap().put(vertexId, edgeMap);
				return true;
			}
		});
		
		socket.send(Message.convertMessageToBytes(repMsg), 0);
	}
	
	public static void processWriteFileRequest(Socket socket, WriteFileReqMsg reqMsg) throws IOException {
		Message writeResponse = ImgpFileTools.processWriteRequest(reqMsg);
		socket.send(Message.convertMessageToBytes(writeResponse), 0);
	}
	
	public static void processLocal2HopRequest(Socket socket) throws IOException {
		Local2HopNeighborProcessor local2HopProc = new Local2HopNeighborProcessor();
		
		int updResponse = local2HopProc.updateLocal2HopNeighbors();
		Message upd1HNResponse = new Message(MessageType.UPD_2HOP_NEIGHBORS_REP);
		upd1HNResponse.setBody((updResponse==1)?"OK":"ERROR");
		
		socket.send(Message.convertMessageToBytes(upd1HNResponse), 0);
		
	}

	
	public static void processClusterAddressRequest(Socket socket) throws IOException {
		ClusterAddressesRep addressRep = new ClusterAddressesRep();
		addressRep.setAddressesIp(StorageTools.getAddressesIps());
		socket.send(Message.convertMessageToBytes(addressRep), 0);
	}
	
	public static void processAddressVertexRequest(Socket socket, AddressVertexReqMsg reqMsg) throws IOException {
		AddressVertexRepMsg response = new AddressVertexRepMsg();
		if(reqMsg != null){
			for (Long cellId : reqMsg.getCellIds())
				response.getCellAddresses().put(cellId, StorageTools.getCellAddress(cellId));
			socket.send(Message.convertMessageToBytes(response), 0);
		}
		else
			System.out.println("No Vertex found");
	}
	
	public static void processLocalVertexIdRequest(Socket socket, LocalVertexIdReqMsg reqMsg) throws IOException {
		LocalVertexIdRepMsg response = new LocalVertexIdRepMsg();
		List<Long> list = response.getCellIds();
		if(reqMsg != null){
//			for (Long cellId : reqMsg.getCellIds())
//				response.getCellAddresses().put(cellId, StorageTools.getCellAddress(cellId));
			Cache<Long, Cell> cellCache = CacheContainer.getCellCache();
			for (Cell cell : cellCache.values()){
				if (cell.getCellType().equals(CellType.VERTEX))
					list.add(cell.getId());
			}
			response.setCellIds(list);
			socket.send(Message.convertMessageToBytes(response), 0);
		}
		else
			System.out.println("No Vertex found");
	}
	
	public static void processCellNumberRequest(Socket socket) throws IOException {
		Message response = new Message(MessageType.NUMBER_OF_CELLS_REP);
		response.setBody(String.valueOf(CacheContainer.getCellCache().getAdvancedCache().getDataContainer().size()));
		socket.send(Message.convertMessageToBytes(response), 0);
	}
	
	public static void processUpdate2HNRequest(Socket socket, Update2HNReqMsg update2HNReqMsg) throws IOException {
		System.out.println("processUpdate2HNRequest");
		//TODO 
		//FIXME ???????
		Map<String, List<Long>> cellsIdMap = TestTools.getCellsID();
		
		for(Entry<String, List<Long>> entry : cellsIdMap.entrySet()){
			for (Long id : entry.getValue()){
				ImgraphGraph.getInstance().getRawGraph().retrieveCell(id);
			}
		}
		Local2HopNeighborUpdater.processUpdateRequest(update2HNReqMsg);
		IdentifiableMessage response = new IdentifiableMessage(MessageType.UPD_2HN_TRANSACTION_REP);
		response.setBody("OK");
		response.setId(update2HNReqMsg.getId());
		socket.send(Message.convertMessageToBytes(response), 0);
	}	
}