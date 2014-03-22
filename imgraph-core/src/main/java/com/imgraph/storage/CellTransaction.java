package com.imgraph.storage;



import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.zeromq.ZMQ;

import com.imgraph.common.Configuration;
import com.imgraph.common.Configuration.Key;
import com.imgraph.index.ImgIndex;
import com.imgraph.index.NeighborhoodVector;
import com.imgraph.model.Cell;
import com.imgraph.model.CellType;
import com.imgraph.model.ImgEdge;
import com.imgraph.model.ImgGraph;
import com.imgraph.model.ImgVertex;
import com.imgraph.networking.messages.LocalVectorUpdateReqMsg;
import com.imgraph.networking.messages.Message;


/**
 * @author Aldemar Reynaga
 * Main class for Imgraph transactions, contains the pending operations and the logic of commit and rollbacks
 */
public class CellTransaction {

	private Map<Long, CellOperations> transactionCells;
	private Set<Long> removedCellIds;
	private Map<String, IndexOperation<Cell>> indexOperations;
	
	public static enum TransactionConclusion {
		COMMIT,
		ROLLBACK
	}

	public CellTransaction() {
		transactionCells = new HashMap<Long, CellOperations>();
	}
	
	private Set<Long> getRemovedCellIds() {
		if (removedCellIds == null)
			removedCellIds = new HashSet<Long>();
		return removedCellIds;
	}
	
	private Map<String, IndexOperation<Cell>> getIndexOperations() {
		if (indexOperations == null)
			indexOperations = new HashMap<String, IndexOperation<Cell>>();
		return indexOperations;
	}
	
	private <T extends Cell> String makeIndexOperationKey(String key, T cell) {
		return key + "::" + cell.getClass().getSimpleName();
	}
	
	public <T extends Cell> void putKeyValueIndex(String indexName, String key, Object value, T cell) {
		getIndexOperation(indexName, key, cell).addKeyValue(key, value, cell);
	}
	
	public <T extends Cell> void removeKeyValueIndex(String indexName, String key, Object value, T cell) {
		getIndexOperation(indexName, key, cell).removeKeyValue(key, value, cell);
	}
	
	@SuppressWarnings("unchecked")
	private <T extends Cell> IndexOperation<Cell> getIndexOperation(String indexName, String key, T cell) {
		String indexOpKey = makeIndexOperationKey(key, cell);
		IndexOperation<Cell> indexOperation = getIndexOperations().get(indexOpKey);
		
		if (indexOperation == null) {
			indexOperation = new IndexOperation<Cell>(indexName, (Class<Cell>) cell.getClass());
			getIndexOperations().put(indexOpKey, indexOperation);
		}
		return indexOperation;
	}
	
	public void addCell(Cell cell) {
		if (transactionCells.containsKey(cell.getId()))
			throw new RuntimeException("The cell is already in the transaction");
		transactionCells.put(cell.getId(), new CellOperations(cell));
	}
	
	public Cell getCell(long cellId) {
		CellOperations cellOperations = transactionCells.get(cellId);
		
		if (cellOperations != null)
			return cellOperations.getCell();
		return null;
	}
	
	public void removeCell(Cell cell) {
		getRemovedCellIds().add(cell.getId());
		transactionCells.get(cell.getId()).setRemovedCell(true);
	}
	
	
	private CellOperations getCellOperations(Cell cell) {
		CellOperations cellOperations =  transactionCells.get(cell.getId());
		if (cellOperations == null) {
			if (!cell.getCellType().equals(CellType.EDGE)) {
				throw new RuntimeException("The " + cell.getCellType().toString() + " with Id " + 
						cell.getId() + " is not included in the transaction");
			} else {
				cellOperations = new CellOperations(cell);
				transactionCells.put(cell.getId(), cellOperations);
			}
		}
		return cellOperations;
	}
	
	public void createCell(Cell cell) {
		CellOperations cellOperations = new CellOperations(cell);
		cellOperations.addOperationType(CellOperationType.CREATE_CELL);
		transactionCells.put(cell.getId(), cellOperations);
	}

	public void addEdge(Cell cell, ImgEdge edge) {
		CellOperations cellOperations = getCellOperations(cell); 
		
		if (!getRemovedCellIds().contains(cell.getId()) && !cellOperations.getTypes().contains(CellOperationType.CREATE_CELL)) {
			cellOperations.addOperationType(CellOperationType.ADD_EDGE);
			cellOperations.getNewEdges().add(edge);
		}
	}
	
	public void removeEdge(Cell cell, ImgEdge edge) {
		CellOperations cellOperations = getCellOperations(cell);
		
		if (!getRemovedCellIds().contains(cell.getId())) {
			getRemovedCellIds().add(edge.getId());
			cellOperations.addOperationType(CellOperationType.REMOVE_EDGE);
			cellOperations.getRemovedEdges().add(edge);
		}
	}
	
	public void setCellProperty(Cell cell, int keyIndex, Object newValue, Object oldValue) {
		CellOperations cellOperations = getCellOperations(cell);
		
		if (!getRemovedCellIds().contains(cell.getId())) {
			cellOperations.addOperationType(CellOperationType.SET_CELL_PROPERTY);
			cellOperations.getSetProperties().put(keyIndex, new CellOperations.ModifiedValue(oldValue, newValue));
		}
	}

	public void removeCellProperty(Cell cell, int keyIndex) {
		CellOperations cellOperations = getCellOperations(cell);
		
		if (!getRemovedCellIds().contains(cell.getId())) {
			cellOperations.addOperationType(CellOperationType.REMOVE_CELL_PROPERTY);
			cellOperations.getRemovedProperties().add(keyIndex);
		}
	}
	
	
	private void executeCellOperation(Cache<Long, Object> cache,  
			CellOperations cellOp) {
		
		if (!cellOp.getCell().getCellType().equals(CellType.EDGE) &&
				!getRemovedCellIds().contains(cellOp.getCellId()) ) {
			ImgGraph graph = ImgGraph.getInstance();
			cellOp.getCell().trimToSize();
			
			graph.storeCell(cellOp.getCellId(), cellOp.getCell());
		} 
		
	}
	
	private void updateIndexes () {
		for (IndexOperation<Cell> indexOperation : getIndexOperations().values()) {
			ImgIndex<Cell> index = ImgGraph.getInstance().getIndex(indexOperation.getIndexName(), indexOperation.getClassName());
			index.commitChanges(indexOperation);
		}
	}
	
	public void commit() {
		Cache<Long, Object> cache = CacheContainer.getCellCache();
		List<Long> cellList = new ArrayList<Long>();
		TransactionManager tm = null;
		Local2HopNeighborUpdater local2HNUpdater = null;
		if (Configuration.getProperty(Key.USE_JTA_TRANSACTIONS).equals("true"))
			tm = cache.getAdvancedCache().getTransactionManager();
		try {
			if (tm != null)
				tm.begin();
				
			if (!transactionCells.isEmpty()) {
				for (CellOperations cellOp: transactionCells.values())
					if (!cellOp.getTypes().isEmpty()){
						cellList.add(cellOp.getCellId());
						executeCellOperation(cache,cellOp);
					}
			}
			
			for (Long cellId : getRemovedCellIds()){
				cellList.add(cellId);
				cache.remove(cellId);
			}

			local2HNUpdater = new Local2HopNeighborUpdater();
			local2HNUpdater.update2HNList(transactionCells);
			
			updateIndexes();
			
			if (tm != null)
				tm.commit();
				
		} catch (Exception e) {
			if (tm != null)
				try {
					tm.rollback();
				} catch (Exception tranException) {
					throw new RuntimeException(tranException);
				}
			throw new RuntimeException(e);
		}
				
		closeTransaction();
		
		/**
		 * Divide cellList depending on the machine the cell is stored
		 *    - Stored on this machine : update its neighborhood vector
		 *    - Otherwise : Send message to other machines to update their modified cells
		 */
		List<Long> distantIds = new ArrayList<Long>();
		for (Long id : cellList){
			ImgVertex vertex = (ImgVertex) CacheContainer.getCellCache().get(id);
			if (vertex != null){
				if (StorageTools.getCellAddress(id).equals(CacheContainer.getCellCache().getCacheManager().getAddress().toString()))
					NeighborhoodVector.updateFullNeighborhoodVector(vertex);//can be null
				else
					distantIds.add(id);
			}
		}
		
		Map<String, String> clusterAddresses = StorageTools.getAddressesIps();
		ZMQ.Socket socket = null;
		ZMQ.Context context = ImgGraph.getInstance().getZMQContext();
		
		try {
			for (Entry<String, String> entry : clusterAddresses.entrySet()) {
				//Only send this message to other machines
				if(!entry.getKey().equals(CacheContainer.getCellCache().getCacheManager().getAddress().toString())){
					socket = context.socket(ZMQ.REQ);
					
					socket.connect("tcp://" + entry.getValue() + ":" + 
							Configuration.getProperty(Configuration.Key.NODE_PORT));
				
					LocalVectorUpdateReqMsg message = new LocalVectorUpdateReqMsg();
					
					message.setCellIds(distantIds);
					
					socket.send(Message.convertMessageToBytes(message), 0);
					
					Message.readFromBytes(socket.recv(0));
					
					socket.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (socket !=null)
				socket.close();
		}
	}

	public void rollback() {
		closeTransaction();
	}

	private void closeTransaction() {
		for (CellOperations cop : transactionCells.values())
			cop.clear();

		if (transactionCells != null)
			transactionCells.clear();
		
		if (removedCellIds != null)
			removedCellIds.clear();
		
		if (indexOperations != null)
			indexOperations.clear();
		
		CellTransactionThread.unset();
	}
}
