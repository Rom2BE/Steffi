package com.imgraph.testing;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.procedure.TIntObjectProcedure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.infinispan.Cache;
import org.zeromq.ZMQ;

import com.imgraph.BasicConsole;
import com.imgraph.common.BigTextFile;
import com.imgraph.common.Configuration;
import com.imgraph.index.NeighborhoodVector;
import com.imgraph.index.Tuple;
import com.imgraph.model.Cell;
import com.imgraph.model.EdgeType;
import com.imgraph.model.ImgEdge;
import com.imgraph.model.ImgGraph;
import com.imgraph.model.ImgVertex;
import com.imgraph.networking.messages.LocalVertexIdRepMsg;
import com.imgraph.networking.messages.LocalVertexIdReqMsg;
import com.imgraph.networking.messages.Message;
import com.imgraph.storage.CacheContainer;
import com.imgraph.storage.StorageTools;
import com.imgraph.storage.CellTransaction.TransactionConclusion;
import com.imgraph.traversal.DistributedTraversal;
import com.imgraph.traversal.EdgeTraversalConf;
import com.imgraph.traversal.Evaluation;
import com.imgraph.traversal.MatchEvaluatorConf;
import com.imgraph.traversal.Path;
import com.imgraph.traversal.TraversalResults;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.imgraph.ImgraphGraph;

/**
 * @author Aldemar Reynaga
 * Functions to execute read, write and traversal tests
 */
@SuppressWarnings("deprecation")
public class TestTools {
	
	private static long state = 0xCAFEBABE; // initial non-zero value
	
	public enum TestFileType{
		RANDOM,
		ALL_PATHS,
		NO_PATHS
	}
	

	public static long nextLong(Random rng, long n) {
		// error checking and 2^x checking removed for simplicity.
		long bits, val;
		do {
			bits = (rng.nextLong() << 1) >>> 1;
			val = bits % n;
		} while (bits-val+(n-1) < 0L);
		return val;
	}
	
	
	private static Long genPathTest(Random randomGen, Cache<Long, Cell> cache, 
			ImgVertex startVertex, int hops, EdgeType edgeType) throws Exception {
		
		int index;
		ImgVertex vertex = startVertex;
		
		
		for (int i=0; i<hops; i++) {
			List<ImgEdge> edges = vertex.getEdgesByType(edgeType);
			if (edges.isEmpty()) {
				return null;
			}
				
				
			index = randomGen.nextInt(edges.size());

			vertex = (ImgVertex) cache.get(edges.get(index).getDestCellId());
		}
		
		if (vertex.getId() == startVertex.getId())
			return null;
		
		return vertex.getId();
	}
	
	
	
	public static void genPathTestFile(long minId, long maxId, int numTests, String outFile,
			int hops, EdgeType edgeType) throws Exception {
		BufferedWriter writer = null;
		Cache<Long, Cell> cache = CacheContainer.getCellCache();
		long startId=0;
		Long endId = null;
		Cell startCell=null;
		Random randomGen = new Random();

		try {
			writer = new BufferedWriter(new FileWriter(new File(
					outFile), false));

			writer.write("#START_ID, END_ID");
			
			for (int i=0; i<numTests; i++) {

				do {
					do {
						startId = nextLong(randomGen, maxId - minId) + minId;
						startCell = cache.get(startId);
					} while (startCell == null);
					endId = genPathTest(randomGen, cache, (ImgVertex) startCell, hops, edgeType);
				} while (endId == null);
				
				writer.newLine();
				writer.write(startId + "," + endId);
			}

		} finally {
			if (writer != null) {
				writer.flush();
				writer.close();
			}
		}
	}
	
	
	public static void genTestFile(long minId, long maxId, int numTests, String outFile) throws Exception {
		BufferedWriter writer = null;
		Cache<Long, Cell> cache = CacheContainer.getCellCache();
		long startId=0, endId=0;
		Cell startCell=null, endCell=null;
		Random randomGen = new Random();

		try {
			writer = new BufferedWriter(new FileWriter(new File(
					outFile), false));

			writer.write("#START_ID, END_ID");
			
			for (int i=0; i<numTests; i++) {
				System.out.println("int i = " + i);
				while (startCell == null) {
					System.out.println("in startCell");
					startId = nextLong(randomGen, maxId - minId) + minId;
					startCell = cache.get(startId);
					System.out.println("out startCell");
				}
				
				while (endCell == null || startCell.getId() == endCell.getId()) {
					System.out.println("in endCell");
					endId = nextLong(randomGen, maxId - minId) + minId;
					endCell = cache.get(endId);
					System.out.println("out endCell");
				}
				writer.newLine();
				writer.write(startId + "," + endId);
				startCell = null;
				endCell = null;
			}
		} finally {
			if (writer != null) {
				writer.flush();
				writer.close();
			}
		}
	}
	
	public static void genVertice(long id, List<ImgEdge> edges, TIntObjectMap<Object> attributes){
		ImgraphGraph graph = ImgraphGraph.getInstance();
		
		if (graph.getRawGraph().retrieveCell(id) == null){
			graph.startTransaction();
			
			ImgVertex vertex = graph.getRawGraph().addVertex(id, "Vertex "+id);
			vertex.putAttributes(attributes);
			graph.stopTransaction(Conclusion.SUCCESS);
			System.out.println("ID "+id+", stored @ " + StorageTools.getCellAddress(id));
		}
		
		graph.startTransaction();
		for(ImgEdge edge : edges)
			((ImgVertex) graph.getRawGraph().retrieveCell(id)).addEdge(((ImgVertex) graph.getRawGraph().retrieveCell(edge.getDestCellId())), true, "Friend");
		graph.commit();
	}
	
	public static void genVertices(long minId, long maxId, long numVertices){
		long i = 1;
		long id, security = 0;
		long securityLoop = minId-1;
		
		Vertex v = null;
		Random randomGen = new Random();
		ImgraphGraph graph = ImgraphGraph.getInstance();
		graph.registerItemName("Name");
		graph.registerItemName("Size");
		graph.registerItemName("Weight");
		while(i<numVertices+1){
			do{
				if(security > maxId){ //prevent infinite loop if wrong input ranges
					securityLoop++;
					id = securityLoop;
				}
				else{
					if (minId == maxId)
						id = minId;
					else
						id = TestTools.nextLong(randomGen, maxId - minId) + minId;
					security++;
				}
				v = graph.getVertex(id); //check if this id is already used 
			} while (v != null); 
			//System.out.println("ID : " + id);
			security = 0;
			securityLoop = minId-1;
			graph.startTransaction();
			
			ImgVertex vertex = graph.getRawGraph().addVertex(id, "Vertex "+id);
			//if ((i%10) == 0)
				vertex.putAttribute("Size", 20 + random(300));
			//vertex.putAttribute("Weight", 40 + random(100));
			/*
			int size = 120+randomGen.nextInt(100);
			vertex.putAttribute("Size", size);
			vertex.putAttribute("Weight", size/2);
			*/
			graph.stopTransaction(Conclusion.SUCCESS);
			i++;
			//
			//if ((i%10000) == 0)
			//	System.out.println(i+"/"+numVertices+" created");
		}
	}
	
	

	public static final long nextLong() {
	  long a=state;
	  state = xorShift64(a);
	  return a;
	}

	public static final long xorShift64(long a) {
	  a ^= (a << 21);
	  a ^= (a >>> 35);
	  a ^= (a << 4);
	  return a;
	}

	public static final int random(int n) {
	  if (n<0) throw new IllegalArgumentException();
	  long result=((nextLong()>>>32)*n)>>32;
	  return (int) result;
	}
	
	public static void genEdges(long minId, long maxId,
			int numEdges, boolean directed) {
		
		ImgraphGraph graph = ImgraphGraph.getInstance();
		graph.registerItemName("Friend");
		
		if (NeighborhoodVector.testing){
			graph.startTransaction();
			
			long i = 2;
			while (i <= maxId){
				((ImgVertex) graph.getRawGraph().retrieveCell(i-1)).addEdge(((ImgVertex) graph.getRawGraph().retrieveCell(i)), directed, "Friend");
				i++;
			}
			graph.commit();
		}
		else {
			long idV1 = 0; //Id of the first vertex
			long idV2 = 0; //Id of the first vertex
			boolean allFull = true; //Checking if all possible edges have already been created in the given range
			boolean edgeAlreadyExist = false; //Checking if there is already an edge between V1 and V2
			boolean fullEdges = false; //Checking if an vertex can create one more edge
			ImgVertex vertex = null;
			
			
			
			//Get vertices ID -> stored in cellsId
			//Check number of vertices found in the range [minId, maxId]
			int range = 0;
			Map<String, List<Long>> cellsIdMap = getCellsID();
			List<Long> cellsId = new ArrayList<Long>();
			
			for(Entry<String, List<Long>> entry : cellsIdMap.entrySet()){
				for (Long id : entry.getValue()){
					if(id >= minId && id <= maxId){
						range++;
						cellsId.add(id);
					}
				}
			}
			//System.out.println(range + " vertices found in the range ["+minId+","+maxId+"]");
			//long totalTime = 0;
			
			
			//Creating numEdges edges
			for(int i=0; i<numEdges; i++){
				//Checking if all possible edges have already been created in the given range
				//Should only check edges that are connected to nodes in the range : edgesInRange
				allFull = true;
				for (Long id : cellsId){
					int edgesInRange = 0;
					for (ImgEdge edge : ((ImgVertex) graph.getRawGraph().retrieveCell(id)).getEdges()){
						long destId = edge.getDestCellId();
						if (destId >= minId && destId <= maxId){
							edgesInRange++;
						}
					}
					if (edgesInRange != range-1)
						allFull = false;
				}
				if (!allFull) {
					//Get source vertex
					do{
						fullEdges = false;
						idV1 = cellsId.get(new Random().nextInt(cellsId.size()));
						//Only take into account edges that connect vertices in the given range.
						int edgeInRangeCounter = 0;
						for (ImgEdge edge : ((ImgVertex) graph.getRawGraph().retrieveCell(idV1)).getEdges()){
							if(edge.getDestCellId() >= minId && edge.getDestCellId() <= maxId)
								edgeInRangeCounter++;
						}
						if (edgeInRangeCounter == range-1)
							fullEdges=true;
					}while (fullEdges);
					
					//Get destination vertex
					do{		
						fullEdges = false;
						edgeAlreadyExist = false;
						idV2 = cellsId.get(new Random().nextInt(cellsId.size()));
						vertex = (ImgVertex) graph.getRawGraph().retrieveCell(idV2);
						//Only take into account edges that connect vertices in the given range.
						int edgeInRangeCounter = 0;
						for (ImgEdge edge : vertex.getEdges()){
							if(edge.getDestCellId() >= minId && edge.getDestCellId() <= maxId)
								edgeInRangeCounter++;
						}
						
						if (edgeInRangeCounter == range-1)
							fullEdges=true;
						if (!fullEdges){
							for(ImgEdge edge : vertex.getEdges()){
								if (edge.getDestCellId()==idV1)
									edgeAlreadyExist = true;
							}
						}
					}while(idV1 == idV2 || fullEdges || edgeAlreadyExist);
					
					//Add a new edge between these two edges.
					
					//long startTime = System.nanoTime();
					
					graph.startTransaction();
					//v1 & v2 must be included in the transaction
					((ImgVertex) graph.getRawGraph().retrieveCell(idV1)).addEdge(((ImgVertex) graph.getRawGraph().retrieveCell(idV2)), directed, "Friend");
					graph.commit();
					
					//totalTime += (System.nanoTime() - startTime);
					//if ((i%1000) == 0)
					//	System.out.println(i+"/"+numEdges+" created, \telapsed : " + totalTime);
					//System.out.println(idV1 + " & " + idV2 + " are now Friends.");
				}
			}
			if (allFull)
				System.out.println("All possible edges ("+ (range*(range-1)/2) +") have already been created for the "+range+" vertices in the range ["+minId+","+(maxId)+"].");
			//System.out.println("Elapsed time : " + totalTime + "ns");
		}
	}
	
	public static void fullMesh(long minId, long maxId, int numVertices) {
		ImgraphGraph graph = ImgraphGraph.getInstance();
		graph.registerItemName("Friend");
		long totalTime = 0;   
		
		for (long i = minId; i < minId+numVertices; i++){
			for (long j = i+1; j < minId+numVertices; j++){
				long startTime = System.nanoTime(); 
				
				graph.startTransaction();
				((ImgVertex) graph.getRawGraph().retrieveCell(i)).addEdge(((ImgVertex) graph.getRawGraph().retrieveCell(j)), false, "Friend");
				graph.commit();
				
				totalTime += (System.nanoTime() - startTime);
				
				if (i%10 == 0 && j == i+1)
					System.out.println(i+"/"+numVertices+ " done, elapsed time : " + totalTime + "ns");
			}
		}
		
		System.out.println("Elapsed time : " + totalTime + "ns");
	}
	
	/*return a map containing for every vertices :
	 * a map containing for every of its edges :
	 *	    * the id of the dest vertex
	 *		* the name of the machine where the dest vertex is stored
	 * result Map<VertexID, Map<DestVertexID, MachineName>>
	 */
	public static Map<Long, Map<Long, String>> getConnections(long maxID){ //TODO use getCellsID
		Map<Long,Map<Long,String>> resultMap = new TreeMap<Long,Map<Long,String>>();
		Long l;
		ImgVertex v;
		for(l=0L; l<=maxID; l++){
			v = (ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(l);
			if (v!=null){
				Map<Long,String> connectionMap = new TreeMap<Long,String>();
				for(ImgEdge edge : v.getEdges()){
					connectionMap.put(edge.getDestCellId(), StorageTools.getCellAddress(edge.getDestCellId()));
				}
				resultMap.put(v.getId(), connectionMap);
			}
		}
		return resultMap;
	}
	
	public static Map<String, List<Long>> getCellsID(){
		Map<String, List<Long>> result = new TreeMap<String, List<Long>>();
		//TODO method to send a message
		Map<String, String> clusterAddresses = StorageTools.getAddressesIps();
		ZMQ.Socket socket = null;
		ZMQ.Context context = ImgGraph.getInstance().getZMQContext();
		
		try {
			for (Entry<String, String> entry : clusterAddresses.entrySet()) {
				socket = context.socket(ZMQ.REQ);
				
				socket.connect("tcp://" + entry.getValue() + ":" + 
						Configuration.getProperty(Configuration.Key.NODE_PORT));
			
				LocalVertexIdReqMsg message = new LocalVertexIdReqMsg();
				
				socket.send(Message.convertMessageToBytes(message), 0);
				
				LocalVertexIdRepMsg response = (LocalVertexIdRepMsg) Message.readFromBytes(socket.recv(0));
				
				List<Long> results = response.getCellIds();
				
				result.put(entry.getKey(), results);
				socket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (socket !=null)
				socket.close();
		}
		return result;
	}
	
	
	private static void runTraversal(DistributedTraversal traversal, 
			long startVertexId, long endVertexId, ImgGraph graph, 
			List<Long> traversalTimes,
			BufferedWriter writer) throws Exception{

		TraversalResults results = traversal.traverse((ImgVertex) graph.retrieveCell(startVertexId));

		traversalTimes.add(results.getTime());
		if (writer != null) {
			List<Path> paths = results.getPaths();

			writer.write(startVertexId + "," + endVertexId + "," +
					results.getTime());

			if (paths.isEmpty()) {
				writer.write(",N,");
			} else {
				String resPaths = "";
				for (Path path : paths)
					resPaths += path.toString() + "||";
				resPaths = resPaths.substring(0, resPaths.length()-2);
				writer.write(",Y," + resPaths);
			}
			writer.newLine();
			//writer.flush();
		}
	}

	private static Map<StatisticalIndicators, Double> calculateIndicators(List<Long> traversalTimes) {
		Map<StatisticalIndicators, Double> indicators = new HashMap<StatisticalIndicators, Double>();
		Collections.sort(traversalTimes);

		if (traversalTimes.size() % 2 == 0)
			indicators.put(StatisticalIndicators.MEDIAN, (traversalTimes.get((traversalTimes.size()/2) - 1) + 
					traversalTimes.get((traversalTimes.size()/2) + 1))/2D);
		else
			indicators.put(StatisticalIndicators.MEDIAN, traversalTimes.get(traversalTimes.size()/2)/2D);

		indicators.put(StatisticalIndicators.MIN, (double)traversalTimes.get(0));
		indicators.put(StatisticalIndicators.MAX, (double)traversalTimes.get(traversalTimes.size()-1));

		double sum = 0, mean;

		for (long time : traversalTimes) 
			sum += (double)time;

		mean = sum / traversalTimes.size();
		indicators.put(StatisticalIndicators.MEAN, mean);
		indicators.put(StatisticalIndicators.TOTAL, sum);


		sum = 0;
		for (long time : traversalTimes)
			sum += Math.pow(time-mean, 2);

		indicators.put(StatisticalIndicators.DEV_STD, Math.sqrt(sum/(traversalTimes.size()-1)));

		return indicators;
	}

	public static Map<StatisticalIndicators, Double> testTraversalFromFile(ImgGraph graph, EdgeTraversalConf edgeTraversalConf, 
			Evaluation evaluation,
			String fileName,  
			int maxDepth, 
			String outLogFile,
			long delay) throws Exception{
		return testTraversalFromFile(graph, edgeTraversalConf, evaluation, fileName, maxDepth, outLogFile, delay, false);
	}
	
	
	public static Map<StatisticalIndicators, Double> testTraversalFromFile(ImgGraph graph, EdgeTraversalConf edgeTraversalConf, 
			Evaluation evaluation,
			String fileName,  
			int maxDepth, 
			String outLogFile,
			long delay, boolean runConcurrentTest) throws Exception{
		BigTextFile file = null;
		DistributedTraversal traversal = new DistributedTraversal();
		BufferedWriter writer = null;
		List<Long> traversalTimes = new ArrayList<Long>();
		int counter = 0;

		long startVertexId, endVertexId;
		try {
			file = new BigTextFile(fileName);

			traversal.setHops(maxDepth);
			traversal.addEdgeTraversalConfs(edgeTraversalConf);
			MatchEvaluatorConf matchConf =  new MatchEvaluatorConf();
			matchConf.setEvaluation(evaluation);
			traversal.setMatchEvaluatorConf(matchConf);

			if (outLogFile != null) 
				writer = new BufferedWriter(new FileWriter(new File(
						outLogFile), false));

			for (String line : file) {
				if (!line.startsWith("#")) {
					StringTokenizer tokenizer = new StringTokenizer(line, ",");
					startVertexId = Long.parseLong(tokenizer.nextToken());
					endVertexId = Long.parseLong(tokenizer.nextToken());
					matchConf.setCellId(endVertexId);

					runTraversal(traversal, startVertexId, endVertexId, graph, 
							traversalTimes, writer);
					counter++;
					if (!runConcurrentTest)
						System.out.println("Traversal # " + counter + " executed");

					Thread.sleep(delay);
				}
			}

			if (runConcurrentTest)
				return null;
			
			return calculateIndicators(traversalTimes);


		} finally {
			if (file!=null) 
				file.Close();
			if (writer != null) {
				writer.flush();
				writer.close();
			}

		}
	}
	
	
	public static Map<StatisticalIndicators, Double> testReads(String testFile, String outLogFile) throws Exception {
		return testReads(testFile, outLogFile, false);
	}
	
	public static Map<StatisticalIndicators, Double> testReads(String testFile, String outLogFile, boolean runConcurTest) throws Exception {
		BigTextFile file = null;
		BufferedWriter writer = null;
		try {
			file = new BigTextFile(testFile);
			List<Long> cellIds = new ArrayList<Long>();
			List<Long> times = new ArrayList<Long>();
			ImgGraph graph = ImgGraph.getInstance();
			long startTime, endTime;
			
			if (outLogFile != null) { 
				writer = new BufferedWriter(new FileWriter(new File(
						outLogFile), false));
				writer.write("CELL_ID, TIME(nanoseconds)");
			}
			
			for (String line : file) {
				if (!line.startsWith("#")) {
					StringTokenizer tokenizer = new StringTokenizer(line, ",");
					cellIds.add(Long.parseLong(tokenizer.nextToken()));
					cellIds.add(Long.parseLong(tokenizer.nextToken()));
				}
			}
			
			for (long cellId : cellIds) {
				startTime = System.nanoTime();

				graph.retrieveCell(cellId);
				endTime = System.nanoTime();

				times.add(endTime-startTime);
				if (writer != null) {
					writer.newLine();
					writer.write(cellId + "," + (endTime-startTime));
				}
			}
			
			if (runConcurTest)
				return null;
			
			return calculateIndicators(times);
			
		} finally {
			if (file!=null) 
				file.Close();
			if (writer!=null)
				writer.close();
		}
	}
	
	public static Map<StatisticalIndicators, Double> testWrites(String testFile, String outLogFile) throws Exception {
		return testWrites(testFile, outLogFile, false);
	}
	
	public static Map<StatisticalIndicators, Double> testWrites(String testFile, String outLogFile, boolean runConcurTest) throws Exception {
		BigTextFile file = null;
		BufferedWriter writer = null;
		try {
			Random random = new Random();
			file = new BigTextFile(testFile);
			List<Long[]> cellIds = new ArrayList<Long[]>();
			List<Long> times = new ArrayList<Long>();
			ImgGraph graph = ImgGraph.getInstance();
			long startTime, endTime;
			Set<Long> newCellIds = new HashSet<Long>();
			long cellId; 
			
			if (outLogFile != null) 
				writer = new BufferedWriter(new FileWriter(new File(
						outLogFile), false));
			
			int transactionCounter=0;
			for (String line : file) {
				if (!line.startsWith("#")) {
					
					StringTokenizer tokenizer = new StringTokenizer(line, ",");
					boolean isNewId = false;
					
					do {
						cellId = nextLong(random, 50000) + 9999999999L;
						
						if (!newCellIds.contains(cellId)) {
							newCellIds.add(cellId);
							isNewId = true;
						}
					} while (!isNewId);
					
					cellIds.add(new Long[]{cellId, Long.parseLong(tokenizer.nextToken()), 
							Long.parseLong(tokenizer.nextToken())});
				}
			}
			
			if (writer != null)
				writer.write("NEW CELL ID, TIME(nanoseconds)");
			
			
			for (Long[] destCellIds : cellIds) {
				
				
				try {
				
					graph.startTransaction();
					ImgVertex vertexA = (ImgVertex) graph.retrieveCell(destCellIds[1]);
					ImgVertex vertexB = (ImgVertex) graph.retrieveCell(destCellIds[2]);
					//startTime = new Date().getTime();
					startTime = System.nanoTime();
					ImgVertex vertex = graph.addVertex(destCellIds[0], null);
					vertex.addEdge(vertexA, true, null);
					vertex.addEdge(vertexB, true, null);
					graph.stopTransaction(TransactionConclusion.COMMIT);
					transactionCounter++;
				} catch (Exception x) {
					System.out.println("Error on transaction " + (transactionCounter+1) +
							", " + destCellIds[0] + "-" + destCellIds[1]);
					throw new Exception(x);
				}
				
				
				
				endTime = System.nanoTime();
				//endTime = new Date().getTime();
				
				if (writer != null) {
					writer.newLine();
					writer.write(String.valueOf(destCellIds[0]) + "," + (endTime-startTime));
				}
				
				times.add(endTime-startTime);
			}
			
			if (runConcurTest)
				return null;
			
			return calculateIndicators(times);
			
		} finally {
			if (file!=null) 
				file.Close();
			if (writer != null) {
				writer.flush();
				writer.close();
			}
		}
	}	
	

	public static void runWriteClients (int numberOfClients, String fileName) {
		long startTime, endTime;
		
		startTime = new Date().getTime();
		
		ExecutorService executorService =  Executors.newFixedThreadPool(numberOfClients);
		
		for (int i = 0; i < numberOfClients; i++) {
	      Runnable client = new WriteClient(fileName);
	      executorService.execute(client);
	    }
		
		executorService.shutdown();
		while (!executorService.isTerminated()) {
			
	    }
		
		endTime = new Date().getTime();
		
		System.out.println("Total time (ms): " + (endTime - startTime));
		
		System.out.println("Writes per second: " +  ((numberOfClients*200*1000)/(endTime - startTime)));
	}
	
	
	public static void runReadClients (int numberOfClients, String fileName) {
		long startTime, endTime;
		
		startTime = new Date().getTime();
		
		ExecutorService executorService =  Executors.newFixedThreadPool(numberOfClients);
		
		for (int i = 0; i < numberOfClients; i++) {
	      Runnable client = new ReadClient(fileName);
	      executorService.execute(client);
	    }
		
		executorService.shutdown();
		while (!executorService.isTerminated()) {
			
	    }
		
		endTime = new Date().getTime();
		
		System.out.println("Total time (ms): " + (endTime - startTime));
		
		System.out.println("Reads per second: " +  ((numberOfClients*200*1000)/(endTime - startTime)));
	}
	
	
	private static List<NodePair> readQueries(String queryFileName) throws Exception {
		List<NodePair> queries =  new ArrayList<NodePair>();
		BigTextFile file = null;
		try {
			file = new BigTextFile(queryFileName);
			for (String line : file) {
				if (!(line.trim().equals("") || line.startsWith("#"))) {
					StringTokenizer tokenizer = new StringTokenizer(line, ",");
					queries.add(new NodePair(Long.parseLong(tokenizer.nextToken()), 
							Long.parseLong(tokenizer.nextToken())));
				}
			}
		} finally {
			if (file != null) file.Close();
		}
		
		
		return queries;
	}
	
	public static void runTraversalClients(int numberOfClients, String queryFileName, int hops, 
			EdgeType edgeType, long testDuration) throws Exception {
		long numberOfTraversals;
		
		
		
		ExecutorService executorService =  Executors.newFixedThreadPool(numberOfClients);
		List<TraversalClient> clients = new ArrayList<TraversalClient>();
		
		List<NodePair> queries = readQueries(queryFileName);
		EdgeTraversalConf edgeTraversalConf = new EdgeTraversalConf("", edgeType);
		
		ImgGraph graph =ImgGraph.getInstance();
		
		
		for (int i = 0; i < numberOfClients; i++) 
			clients.add(new TraversalClient(hops, edgeTraversalConf, queries, graph));
			
	    //Launch client threads
		for (Runnable client : clients)
			executorService.execute(client);
		
		Thread.sleep(testDuration);
		numberOfTraversals = 0;
		for (TraversalClient client : clients) {
			client.stop();
			numberOfTraversals += client.getCounter();
		}
		
		
		executorService.shutdown();
		while (!executorService.isTerminated()) {
			
	    }
		
		System.out.println("Traversals per second: " +  ((1000*numberOfTraversals)/testDuration));
		
		
	}


	public static List<TreeSet<Long>> manualJoinSearch(
			ImgraphGraph graph,
			int joinElements,
			List<Tuple<String, String>> userInput,
			List<Tuple<Tuple<String, Object>, List<Tuple<Long, Integer>>>> tuplesList) {
		
		//This HashMap called result will contain vertices laying in the neighborhoods of each researched value.
		Map<Long, List<Tuple<String, Tuple<Object, Integer>>>> result = new HashMap<Long, List<Tuple<String, Tuple<Object, Integer>>>>();
		
		//First tuple : add everything
		for (Tuple<Long, Integer> intensities1 : tuplesList.get(0).getY()){
			List<Tuple<String, Tuple<Object, Integer>>> list = new ArrayList<Tuple<String, Tuple<Object, Integer>>>();
			list.add(new Tuple<String, Tuple<Object, Integer>>(tuplesList.get(0).getX().getX(), new Tuple<Object, Integer>(tuplesList.get(0).getX().getY(), intensities1.getY())));
			result.put(intensities1.getX(), list);
		}
		
		//Other tuples : add only if already present
		for(int i = 1; i<joinElements; i++){
			for (Tuple<Long, Integer> intensities : tuplesList.get(i).getY()){
				if (result.keySet().contains(intensities.getX())){
					List<Tuple<String, Tuple<Object, Integer>>> list = result.get(intensities.getX());
					list.add(new Tuple<String, Tuple<Object, Integer>>(tuplesList.get(i).getX().getX(), new Tuple<Object, Integer>(tuplesList.get(i).getX().getY(), intensities.getY())));
					result.put(intensities.getX(), list);
				}
			}
		}
		
		
	
		//Clean if not present in all tuples
		List<Long> idToRemove = new ArrayList<Long>();
		for (Entry<Long, List<Tuple<String, Tuple<Object, Integer>>>> entry : result.entrySet()){
			if (entry.getValue().size() < joinElements)
				idToRemove.add(entry.getKey());
		}
		for (Long id : idToRemove)
			result.remove(id);
		
		List<TreeSet<Long>> rawResults = new ArrayList<TreeSet<Long>>();
		List<List<Long>> expendedIds = new ArrayList<List<Long>>();
		boolean unreachable = false;
		
		//Fill expendedIds with values indexed intensities list.
		for(int i = 0; i<joinElements; i++){
			List<Long> ids = new ArrayList<Long>();
			for (Tuple<Long, Integer> tuple : tuplesList.get(i).getY())
				ids.add(tuple.getX());
			
			expendedIds.add(ids);
		}
		
		List<Long> pivots = BasicConsole.updatePivots(expendedIds);
		
		if (joinElements > 1){
			while (BasicConsole.checkPivotInAllLists(pivots, expendedIds) == null && !unreachable){ //null if no pivots present in all Lists
				int id = BasicConsole.getListIdNoPivots(pivots, expendedIds);//id of a list containing no pivots
				
				if (id == -1){
					id = BasicConsole.getListIdLessPivots(pivots, expendedIds);//id of a list containing less pivots than other lists
				}
				
				List<Long> expendedList = BasicConsole.expend(expendedIds.get(id));
				
				boolean containAll = true;
				for (Long oldId : expendedList){
					if (!expendedIds.get(id).contains(oldId))
						containAll = false;
				}
				
				if (containAll)
					unreachable = true;
				
				expendedIds.remove(id);
				expendedIds.add(expendedList);
				
				pivots = BasicConsole.updatePivots(expendedIds);
				
			}
		}
		
		if (!unreachable){
			
			//Only keep pivots present in all neighborhoods
			List<Long> finalPivots = new ArrayList<Long>();
			boolean presentInAll;
			for (Long id : pivots){
				presentInAll = true;
				for (List<Long> list : expendedIds){
					if (!list.contains(id))
						presentInAll = false;
				}
				if (presentInAll)
					finalPivots.add(id);
			}
			//System.out.println("Complete pivots : "+finalPivots);
			//System.out.println("ExpendedIds : "+expendedIds);
		
			TreeSet<Long> fullVertices = new TreeSet<Long>(expendedIds.get(0));
			if (joinElements > 1){
				for (int i = 1; i < joinElements; i++){
					List<Long> list = expendedIds.get(i);
					for (Long newId : list){
						if (!fullVertices.contains(newId))
							fullVertices.add(newId);
					}
				}
			}
			
			boolean modified = true;
			if (joinElements > 1){
				while (modified){
					modified = false;
					List<Long> copy = new ArrayList<Long>(fullVertices);
					List<Long> removedIds = new ArrayList<Long>();
					
					for (Long id : fullVertices){
						copy.remove(id);
						
						if (copy.size()!=0 && BasicConsole.checkConnected(copy) && BasicConsole.checkValuesPresent(joinElements, tuplesList, copy)){
							modified = true;
							removedIds.add(id);
						}
						else
							copy.add(id);
					}
					for(Long id : removedIds)
						fullVertices.remove(id);
				}
			}

			//Avoid duplicates
			if (!rawResults.contains(fullVertices))
				rawResults.add(fullVertices);
		
	
			//REMOVE RESULTS BIGGER THAN OTHERS
			if (rawResults.size() != 0){
				modified = true;
				int size = rawResults.get(0).size();
	
				while (modified){
					modified = false;
					List<TreeSet<Long>> toRemove = new ArrayList<TreeSet<Long>>();
					for (TreeSet<Long> list : rawResults){
						if (list.size() > size)
							toRemove.add(list);
						else if (list.size() < size){
							size = list.size();
							modified = true;
						}
					}
	
					for (TreeSet<Long> list : toRemove)
						rawResults.remove(list);
				}
			}
		}
		else 
			rawResults = null;
		//PRINT FINAL RESULTS
		/*
		if (rawResults.size() < 2)
			System.out.println("\nSearch result : "+rawResults);
		else
			System.out.println("\nSearch results : "+rawResults);
		*/
		
		return rawResults;
		//System.out.println("Elapsed time for manual search: " + (System.nanoTime() - startTime) + "ns");
		
	}


	public static List<TreeSet<Long>> joinMultipleSearch(
			ImgraphGraph graph,
			int joinElements,
			List<Tuple<Tuple<String, Object>, List<Tuple<Long, Integer>>>> tuplesList) {
	
		//This HashMap called result will contain vertices laying in the neighborhoods of each researched value.
		Map<Long, List<Tuple<String, Tuple<Object, Integer>>>> completePivots = new HashMap<Long, List<Tuple<String, Tuple<Object, Integer>>>>();
		
		//First tuple : add everything
		for (Tuple<Long, Integer> intensities1 : tuplesList.get(0).getY()){
			List<Tuple<String, Tuple<Object, Integer>>> list = new ArrayList<Tuple<String, Tuple<Object, Integer>>>();
			list.add(new Tuple<String, Tuple<Object, Integer>>(tuplesList.get(0).getX().getX(), new Tuple<Object, Integer>(tuplesList.get(0).getX().getY(), intensities1.getY())));
			completePivots.put(intensities1.getX(), list);
		}
		
		//Other tuples : add only if already present
		for(int i = 1; i<joinElements; i++){
			for (Tuple<Long, Integer> intensities : tuplesList.get(i).getY()){
				if (completePivots.keySet().contains(intensities.getX())){
					List<Tuple<String, Tuple<Object, Integer>>> list = completePivots.get(intensities.getX());
					list.add(new Tuple<String, Tuple<Object, Integer>>(tuplesList.get(i).getX().getX(), new Tuple<Object, Integer>(tuplesList.get(i).getX().getY(), intensities.getY())));
					completePivots.put(intensities.getX(), list);
				}
			}
		}
	
		//Clean if not present in all tuples
		List<Long> idToRemove = new ArrayList<Long>();
		for (Entry<Long, List<Tuple<String, Tuple<Object, Integer>>>> entry : completePivots.entrySet()){
			if (entry.getValue().size() < joinElements)
				idToRemove.add(entry.getKey());
		}
		for (Long id : idToRemove)
			completePivots.remove(id);
		
		List<TreeSet<Long>> rawResults;
		//TODO as explained in the thesis
		//result is not empty
		boolean unreachable = false;
		
		if (completePivots.size() != 0){
			if (joinElements > 1)
				rawResults = BasicConsole.buildConnectedGraph(graph, joinElements, tuplesList, new ArrayList(completePivots.keySet()), true);
			else{
				rawResults = new ArrayList<TreeSet<Long>>();
				for (Entry<Long, List<Tuple<String, Tuple<Object, Integer>>>> entry : completePivots.entrySet()){
					//System.out.println(entry.getKey() + " : " + entry.getValue());
					
					String researchedAttribute = tuplesList.get(0).getX().getX();
					Object researchedValue = tuplesList.get(0).getX().getY();
					
					ImgVertex vertex = (ImgVertex) graph.getRawGraph().retrieveCell(entry.getKey());
					Object value = vertex.getAttribute(researchedAttribute);
					
					//This vertice contain this attribute
					if (value != null){
						//This vertice contains the researched value
						if (value.equals(researchedValue)){
							TreeSet<Long> results = new TreeSet<Long>();
							results.add(entry.getKey());
							rawResults.add(results);
						}
					}
				}
			}
		}
		//result is empty, the neighborhoods are not overlapping
		else{
			
			List<List<Long>> expendedIds = new ArrayList<List<Long>>();
			
			//Fill expendedIds with values indexed intensities list.
			for(int i = 0; i<joinElements; i++){
				List<Long> ids = new ArrayList<Long>();
				for (Tuple<Long, Integer> tuple : tuplesList.get(i).getY())
					ids.add(tuple.getX());
				
				expendedIds.add(ids);
			}
			List<Long> pivots = BasicConsole.updatePivots(expendedIds);
			if (joinElements > 1){
				while (BasicConsole.checkPivotInAllLists(pivots, expendedIds) == null && !unreachable){ //null if no pivots present in all Lists
					int id = BasicConsole.getListIdNoPivots(pivots, expendedIds);//id of a list containing no pivots
					
					if (id == -1){
						id = BasicConsole.getListIdLessPivots(pivots, expendedIds);//id of a list containing less pivots than other lists
					}
					
					List<Long> expendedList = BasicConsole.expend(expendedIds.get(id));
					
					boolean containAll = true;
					for (Long oldId : expendedList){
						if (!expendedIds.get(id).contains(oldId))
							containAll = false;
					}
					
					if (containAll)
						unreachable = true;
					
					expendedIds.remove(id);
					expendedIds.add(expendedList);
					
					pivots = BasicConsole.updatePivots(expendedIds);
				}
			}
			
			if (!unreachable){
				//Only keep pivots present in all neighborhoods
				List<Long> expandedCompletePivots = new ArrayList<Long>();
				
				boolean presentInAll;
				for (Long id : pivots){
					presentInAll = true;
					for (List<Long> list : expendedIds){
						if (!list.contains(id))
							presentInAll = false;
					}
					if (presentInAll)
						expandedCompletePivots.add(id);
				}
				rawResults = BasicConsole.buildConnectedGraph(graph, joinElements, tuplesList, expandedCompletePivots, false);
			}
			else
				rawResults = null;
		}
		if (!unreachable){
			//REMOVE RESULTS BIGGER THAN OTHERS
			if (rawResults.size() != 0){
				boolean modified = true;
				int size = rawResults.get(0).size();
				
				while (modified){
					modified = false;
					List<TreeSet<Long>> toRemove = new ArrayList<TreeSet<Long>>();
					for (TreeSet<Long> list : rawResults){
						if (list.size() > size)
							toRemove.add(list);
						else if (list.size() < size){
							size = list.size();
							modified = true;
						}
					}
	
					for (TreeSet<Long> list : toRemove)
						rawResults.remove(list);
				}
			}
		}
		return rawResults;
	}
}