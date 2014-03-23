package com.imgraph.testing;

import gnu.trove.map.TIntObjectMap;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.infinispan.Cache;
import org.zeromq.ZMQ;

import com.imgraph.common.BigTextFile;
import com.imgraph.common.Configuration;
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
			System.out.println("ID : " + id);
			security = 0;
			securityLoop = minId-1;
			graph.startTransaction();
			
			ImgVertex vertex = graph.getRawGraph().addVertex(id, "Vertex "+id);
			int size = 120+randomGen.nextInt(100);
			vertex.putAttribute("Size", size);
			vertex.putAttribute("Weight", size/2);
			
			graph.stopTransaction(Conclusion.SUCCESS);
			i++;
		}
	}
	
	//TODO error when full edges
	public static void genEdges(long minId, long maxId,
			int numEdges, boolean directed) {
		ImgraphGraph graph = ImgraphGraph.getInstance();
		graph.registerItemName("Friend");
		long idV1 = 0; //Id of the first vertex
		long idV2 = 0; //Id of the first vertex
		boolean allFull = true; //Checking if all possible edges have already been created in the given range
		boolean edgeAlreadyExist = false; //Checking if there is already an edge between V1 and V2
		boolean fullEdges = false; //Checking if an vertex can create one more edge //TODO should only count edges of vertices in the range
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
		System.out.println(range + " vertices found in the range ["+minId+","+maxId+"]");
		
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
					if (((ImgVertex) graph.getRawGraph().retrieveCell(idV1)).getEdges().size() == range-1)
						fullEdges=true;
				}while (fullEdges);
				
				//Get destination vertex
				do{		
					fullEdges = false;
					edgeAlreadyExist = false;
					idV2 = cellsId.get(new Random().nextInt(cellsId.size()));
					vertex = (ImgVertex) graph.getRawGraph().retrieveCell(idV2);
					if (vertex.getEdges().size() == range-1)
						fullEdges=true;
					if (!fullEdges){
						for(ImgEdge edge : vertex.getEdges()){
							if (edge.getDestCellId()==idV1)
								edgeAlreadyExist = true;
						}
					}
				}while(idV1 == idV2 || fullEdges || edgeAlreadyExist);
				
				//Add a new edge between these two edges.
				
				graph.startTransaction();
				
				((ImgVertex) graph.getRawGraph().retrieveCell(idV1)).addEdge(((ImgVertex) graph.getRawGraph().retrieveCell(idV2)), directed, "Friend");
				
				graph.commit();

				System.out.println(idV1 + " & " + idV2 + " are now Friends.");
			}
		}
		if (allFull)
			System.out.println("All possible edges ("+ (range*(range-1)/2) +") have already been created for the "+range+" vertices in the range ["+minId+","+(maxId)+"].");
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
}


