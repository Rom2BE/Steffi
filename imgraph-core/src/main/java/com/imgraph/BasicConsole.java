package com.imgraph;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.procedure.TIntObjectProcedure;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.Channel;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.stack.IpAddress;
import org.zeromq.ZMQ;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.imgraph.common.Configuration;
import com.imgraph.common.IOUtils;
import com.imgraph.common.ImgLogger;
import com.imgraph.index.AttributeIndex;
import com.imgraph.index.ImgIndex;
import com.imgraph.index.ImgIndexHits;
import com.imgraph.index.NeighborhoodVector;
import com.imgraph.index.Tuple;
import com.imgraph.loader.LoadVertexInfo;
import com.imgraph.loader.TextFileLoader;
import com.imgraph.model.EdgeType;
import com.imgraph.model.ImgEdge;
import com.imgraph.model.ImgGraph;
import com.imgraph.model.ImgVertex;
import com.imgraph.networking.ClusterConfigManager;
import com.imgraph.networking.NodeServer;
import com.imgraph.networking.messages.AddressVertexRepMsg;
import com.imgraph.networking.messages.AddressVertexReqMsg;
import com.imgraph.networking.messages.LoadMessage;
import com.imgraph.networking.messages.MessageType;
import com.imgraph.networking.messages.LoadMessage.LoadFileType;
import com.imgraph.networking.messages.Message;
import com.imgraph.storage.CacheContainer;
import com.imgraph.storage.FileUtilities;
import com.imgraph.storage.ImgpFileTools;
import com.imgraph.storage.Local2HopNeighbors;
import com.imgraph.storage.StorageTools;
import com.imgraph.testing.ChatActor;
import com.imgraph.testing.StatisticalIndicators;
import com.imgraph.testing.TestTools;
import com.imgraph.traversal.DistributedTraversal;
import com.imgraph.traversal.EdgeTraversalConf;
import com.imgraph.traversal.Evaluation;
import com.imgraph.traversal.MatchEvaluator;
import com.imgraph.traversal.MatchEvaluatorConf;
import com.imgraph.traversal.Method;
import com.imgraph.traversal.Path;
import com.imgraph.traversal.SimpleTraversal;
import com.imgraph.traversal.TraversalResults;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.imgraph.ImgraphEdge;
import com.tinkerpop.blueprints.impls.imgraph.ImgraphGraph;
import com.tinkerpop.blueprints.impls.imgraph.ImgraphVertex;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * @author Aldemar Reynaga
 * Creates a basic text console conceived for the execution of tests on basic functionalities, 
 * this console can be started using the parameter START_CONSOLE from the main JAR file 
 */
@SuppressWarnings("deprecation")
public class BasicConsole {
	
	static ActorSystem system = null;
	static ActorRef chat = null;
	
	public static void runConsoleNoZeroMQ() throws Exception {
		runConsole(false);
	}
	
	public static void runConsole() throws Exception {
		runConsole(true);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void runConsole(boolean initZMQ) throws Exception{
		System.setProperty("java.net.preferIPv4Stack" , "true");
		String command;
		
		NodeServer nodeServer = null;
		
		
		if (initZMQ) {
			nodeServer = new NodeServer();
			new Thread(nodeServer).start();
		}
			
		Thread.sleep(1000);//allows ZMQ server to be initialized before running the command
		CacheContainer.getCacheContainer().start();
		CacheContainer.getCellCache().start();
		ImgraphGraph graph = ImgraphGraph.getInstance();
		//FIXME To go faster
		ClusterConfigManager configManager = new ClusterConfigManager();
		try {
			configManager.initialize();
			System.out.println("The cluster configuration was executed satisfactorily");
		} catch (Exception x) {
			x.printStackTrace();
		} finally {
			configManager.closeClientThreads();
		}
		//FIXME
		
		while (true) {
			System.out.println("\nWaiting for your command :");
			command = IOUtils.readLine(">");
			if (command.equals("load")) {
				String fileName = IOUtils.readLine("Load from file: ");
				
				command = IOUtils.readLine("Load type: ");
				if (command.equals("2")) {
					TextFileLoader loader = new TextFileLoader();
					boolean isDirected = IOUtils.readLine("Directed (Y/N): ").equals("Y");
					LoadFileType loadFileType = 
							IOUtils.readLine("Adjacent List file type (Y/N)?: ").equals("Y")?LoadFileType.ADJ_LIST_TEXT_FILE:
								LoadFileType.SIMPLE_TEXT_FILE;
					try {
						loader.load(fileName, loadFileType, isDirected);
					} catch (Exception e) {
						ImgLogger.logError(e, "Error loading adjacent list format file (parallel");
					} finally {
						loader.close();
					}
					/*
					loader.load(fileName);
					loader.closeLoaderClients();
					*/
				} else if (command.equals("3")) {
					
					try {
						FileUtilities.readFromFile(graph, fileName);
						
					} catch (Exception e) {
						
						ImgLogger.logError(e, "Error loading imgp file (simple)");
					}
				} else if (command.equals("4")) {
					ImgpFileTools fileLoader =  new ImgpFileTools("x");
					try {
						fileLoader.readFromFile(graph, fileName);
					} catch (Exception e) {
						ImgLogger.logError(e, "Error loading imgp file (parallel");
					} finally {
						try { fileLoader.closeClientThreads();}catch(Exception x) {}
					}
				} else if (command.equals("5")) {
					try {
						TextFileLoader.singleProcessLoad(graph, fileName);
					} catch (Exception e) {
						ImgLogger.logError(e, "Error loading ajacent list format file (simple");
					}
				}
			} else if (command.equals("print")) {
				String vertexId  = IOUtils.readLine("Vector Id: ");
				ImgVertex v = (ImgVertex) graph.getRawGraph().retrieveCell(Long.parseLong(vertexId));
				
				System.out.println(v);
			} else if (command.equals("printAll")) {
				
				Map<Long, NeighborhoodVector> vectorsMap = new TreeMap<Long, NeighborhoodVector>();
				
				//Print Vertices and Edges
				Map<String, List<Long>> cellsIdMap = TestTools.getCellsID();
				long maxID = 0;
				for(Entry<String, List<Long>> entry : cellsIdMap.entrySet()){
					for (Long id : entry.getValue()){
						if(id>maxID)
							maxID = id;
					}
				}
				Map<Long, Map<Long, String>> connectionsMap = TestTools.getConnections(maxID);
				String result = "";
				String oneHop = "";
				String twoHop = "";
				Iterator<Entry<Long, Map<Long, String>>> entries = connectionsMap.entrySet().iterator();
				while (entries.hasNext()) {
					result = "";
					Entry<Long, Map<Long, String>> vertexInfo = entries.next();
					Long vertexID = vertexInfo.getKey();
					ImgVertex vertex = (ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(vertexID);
					Map<Long,String> edges = vertexInfo.getValue(); //Edges
					//Id & Location
					result += "Vertex " + vertexID + " : "										
							+ "\n\t - is stored @ " + StorageTools.getCellAddress(vertexID);
					System.out.println(result);
					//Connections (1&2 hops)
					if (edges.size()>0){
						result = "\t - is connected to [";
						oneHop = "";
						twoHop = "";
						for(ImgEdge edge : vertex.getEdges()) {					//Connected To
							if(oneHop.equals(""))
								oneHop += edge.getDestCellId();
							else
								oneHop += ","+edge.getDestCellId();
							
							for (ImgEdge twoHopEdges : ((ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(edge.getDestCellId())).getEdges()){
								if(twoHopEdges.getDestCellId() != vertexID){
									if(twoHop.equals(""))
										twoHop += twoHopEdges.getDestCellId();
									else
										twoHop += ","+twoHopEdges.getDestCellId();
								}
							}
						}
						System.out.println(result+"1H : {"+oneHop+"}, 2H : {"+twoHop+"}]");	
					}	
					//Attributes
					for (String key : vertex.getAttributeKeys()){
						System.out.println("\t - " + key + " : " + vertex.getAttribute(key));
					}
					//Neighborhood vector
					NeighborhoodVector vector = vertex.getNeighborhoodVector();
					vectorsMap.put(vertexID, vector);
					System.out.println(vector);
				}
				
				//Print Machines
				int i = 1;
				int machineCount = 0;
				int totalCount = 0;
				Map<String, Integer> cellCount = StorageTools.countCellsInCluster();
				for (Address address : CacheContainer.getCacheContainer().getTransport().getMembers()){
					for (Entry<String, Integer> entry : cellCount.entrySet()) {
						if (entry.getKey().equals(address.toString()))
							machineCount = entry.getValue();
					}
					System.out.println("Machine "+i+"'s Address : "+ address.toString()
							+", \t IpAddress : "+StorageTools.getIpAddress(address)
							+"\t "+machineCount+" cells.");
					totalCount += machineCount;
					i++;
				}
				System.out.println("Total Cell Count = " + totalCount); 
				//TODO Edge Count

				//Print the attribute index
				System.out.println(graph.getRawGraph().getAttributeIndex());
				
			} else if (command.equals("printPortals")) {
				int vertexCounter = 0;
				int portalCounter = 0;
				boolean isPortal = false;
				
				Map<String, List<Long>> cellsIdMap = TestTools.getCellsID();
				Map<Long, List<Long>> portalsMap = new TreeMap<Long, List<Long>>();
				//Get Information and sort them
				for (Entry<String, List<Long>> entry : cellsIdMap.entrySet()){
					for (Long id : entry.getValue()){
						isPortal = false;
						for (ImgEdge edge : ((ImgVertex) graph.getRawGraph().retrieveCell(id)).getEdges()){
							if (entry.getKey() != StorageTools.getCellAddress(edge.getDestCellId())){
								isPortal = true;
								List<Long> connectedList = null;
								if (portalsMap.containsKey(id)){ //Is already connected to an other vertice
									connectedList = portalsMap.get(id);
								}
								else{ //First connection found
									connectedList = new ArrayList<Long>();
								}
								connectedList.add(edge.getDestCellId());
								portalsMap.put(id, connectedList);
							}
						}
						if (isPortal)
							portalCounter++;
					}
					vertexCounter += entry.getValue().size();
				}
				
				//Print sorted information (more comprehensive)
				for (Entry<Long, List<Long>> entry : portalsMap.entrySet()){
					System.out.println("Vertice "+entry.getKey()
							+", stored @ "+StorageTools.getCellAddress(entry.getKey())+" is connected to :");
					for (Long id : entry.getValue())
						System.out.println("\t - ID :" +id+", stored @ "+StorageTools.getCellAddress(id));
				}
				System.out.println("\n");
				System.out.println("There are " + portalCounter + " portal for " + vertexCounter + " vertices stored on "+graph.getRawGraph().getMemberIndexes().entrySet().size()+" machines.");
				System.out.println("\n");
			} else if (command.equals("2HN")) {
				for (Long cellId : Local2HopNeighbors.getCellIds()) {
					System.out.println("Cell ID: " + cellId);
					for (ImgEdge edge : Local2HopNeighbors.getNeighbors(cellId).getAllEdges())
						System.out.println(edge);
				}
			} else if (command.equals("sedge")) {
				command = IOUtils.readLine("Vector Id: ");
				ImgVertex v = (ImgVertex) graph.getRawGraph().retrieveCell(Long.parseLong(command));
				if (v == null) {
					System.out.println("Vertex not found");
				} else {
					command = IOUtils.readLine("Destination vector Id: ");
					for (ImgEdge edge : v.getEdges())
						if (edge.getDestCellId() == Long.parseLong(command))
							System.out.println("Edge found: " + edge);
				}
				
			} else if (command.equals("lv")) {
				
				String vectorId  = IOUtils.readLine("Vector Id: ");
				
				command  = IOUtils.readLine("IN/OUT/UNDIRECTED: ");
				
				ImgVertex v = (ImgVertex) graph.getRawGraph().retrieveCell(Long.parseLong(vectorId));
				
				if (v == null) {
					System.out.println("Vertex not found");
				} else {
					System.out.println("V: " + v.getId() + " - Machine: " + StorageTools.getCellAddress(v.getId()));
					int i=0;
					for (ImgEdge ie : v.getEdges(EdgeType.valueOf(command))) {
						System.out.print(ie.getDestCellId() + ", ");
						i++;
					}
					
					System.out.println("\nTotal: " + i);
				}
				
				
			} else if (command.equals("localList")) {
				for (Object cell : CacheContainer.getCellCache().values()) {
					System.out.println(cell);
				}
			} else if (command.equals("indexGetTest")) {
				ImgIndex<ImgVertex> vertexIndex = graph.getRawGraph().getDefaultVertexIndex();
				ImgIndex<ImgEdge> edgeIndex = graph.getRawGraph().getDefaultEdgeIndex();
				
				String vertexValue = IOUtils.readLine("Vertex property value: "); 
				String edgeValue = IOUtils.readLine("Edge property value: ");
				
				ImgIndexHits<ImgVertex> indexHits = vertexIndex.get("Country", vertexValue);
				for (ImgVertex iv : indexHits)
					System.out.println(iv);
				
				
				ImgIndexHits<ImgEdge> indexHits2 = edgeIndex.get("TypeOfFriend", edgeValue);
				for (ImgEdge iv : indexHits2)
					System.out.println(iv);
				
			} else if (command.equals("indexAddTest")) {
				try {
					graph.registerItemName("Country");
					graph.registerItemName("Friend");
					graph.registerItemName("TypeOfFriend");
					graph.startTransaction();
					ImgIndex<ImgVertex> vertexIndex = graph.getRawGraph().getDefaultVertexIndex();
					ImgIndex<ImgEdge> edgeIndex = graph.getRawGraph().getDefaultEdgeIndex();
					
					ImgraphVertex v = (ImgraphVertex) graph.addVertex(1);
					v.setProperty("Country", "Bolivia");
					vertexIndex.put("Country", "Bolivia", v.getRawVertex());
					
					ImgraphVertex w = (ImgraphVertex) graph.addVertex(2);
					w.setProperty("Country", "Bolivia");
					vertexIndex.put("Country", "Bolivia", w.getRawVertex());
					
					ImgraphVertex x = (ImgraphVertex) graph.addVertex(3);
					x.setProperty("Country", "Belgica");
					vertexIndex.put("Country", "Belgica", x.getRawVertex());
					
					ImgraphEdge e1 = (ImgraphEdge) v.addEdge("Friend", w);
					e1.setProperty("TypeOfFriend", "Close");
					edgeIndex.put("TypeOfFriend", "Close", e1.getRawEdge());
					
					ImgraphEdge e2 = (ImgraphEdge) w.addEdge("Friend", x);
					e2.setProperty("TypeOfFriend", "NotClose");
					edgeIndex.put("TypeOfFriend", "NotClose", e2.getRawEdge());
					
					ImgraphEdge e3 = (ImgraphEdge) x.addEdge("Friend", v);
					e3.setProperty("TypeOfFriend", "Close");
					edgeIndex.put("TypeOfFriend", "Close", e3.getRawEdge());
					graph.commit();
				
					/*
					ImgIndexHits<ImgVertex> indexHits = vertexIndex.get("Country", "Bolivia");
					for (ImgVertex iv : indexHits)
						System.out.println(iv);
					
					
					ImgIndexHits<ImgEdge> indexHits2 = edgeIndex.get("TypeOfFriend", "Close");
					for (ImgEdge iv : indexHits2)
						System.out.println(iv);
					*/
				} catch (Exception x) {
					x.printStackTrace();
				}
			} else if (command.equals("addCellTest")) {
				try {
					
					graph.registerItemName("name");
					graph.registerItemName("years");
					graph.registerItemName("Friend");
					
					graph.startTransaction();
					
					Vertex v1 = graph.addVertex(1L);
					Vertex v2  = graph.addVertex(2L);
					Edge e = graph.addEdge("", v1, v2, "Friend");
					
					v1.setProperty("name", "Pedro");
					v2.setProperty("name", "Juan");
					e.setProperty("years", 15);
					
					graph.stopTransaction(Conclusion.SUCCESS);
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (command.equals("addCell")) {
				try {
					String vectorId  = IOUtils.readLine("Vector Id: ");
					
					graph.startTransaction();

					graph.addVertex(Long.parseLong(vectorId));
					
					graph.stopTransaction(Conclusion.SUCCESS);
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (command.equals("myTest")) {
				try {
					graph.registerItemName("name");
					graph.registerItemName("years");
					graph.registerItemName("Friend");
					
					graph.startTransaction();
					
					Vertex v1 = graph.addVertex(1L);
					Vertex v2 = graph.addVertex(2L);
					Vertex v3 = graph.addVertex(3L);
					Vertex v4 = graph.addVertex(4L);
					
					System.out.println(StorageTools.getCellAddress((Long) v1.getId()));
					System.out.println(StorageTools.getCellAddress((Long) v2.getId()));
					System.out.println(StorageTools.getCellAddress((Long) v3.getId()));
					System.out.println(StorageTools.getCellAddress((Long) v4.getId()));
					
					Edge e0 = graph.addUndirectedEdge("", v1, v2, "Friend");
					Edge e1 = graph.addUndirectedEdge("", v2, v3, "Friend");
					Edge e2 = graph.addUndirectedEdge("", v3, v4, "Friend");
					Edge e3 = graph.addUndirectedEdge("", v4, v1, "Friend");
					
					v1.setProperty("name", "Romain");
					v2.setProperty("name", "Livie");
					v3.setProperty("name", "Steven");
					v4.setProperty("name", "Antoine");
					
					e0.setProperty("years", 12);
					e1.setProperty("years", 23);
					e2.setProperty("years", 34);
					e3.setProperty("years", 41);
					
					graph.stopTransaction(Conclusion.SUCCESS);					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (command.equals("genVertices")) {
				try {					
					boolean correctInput = true;
					boolean correctRange = true;
					long numVertices = 0;
					long minId = 1;
					long maxId = 0;
					
					command = IOUtils.readLine("Number of vertices: ");
					if (isNumeric(command)){
						numVertices = Long.parseLong(command);
						maxId = numVertices;
					}
					else 
						correctInput = false;
					/*
					if(correctInput){
						command = IOUtils.readLine("Minimum id: ");
						if (isNumeric(command))
							minId = Long.parseLong(command);
						else 
							correctInput = false;
					}
					*/
					if(correctInput){
						/*
						command = IOUtils.readLine("Maximum id: ");
						if (isNumeric(command)){
							maxId = Long.parseLong(command);
							if (maxId < minId)
								correctRange = false;
							else{
							*/
								long startTime = System.nanoTime();
								TestTools.genVertices(minId, maxId, numVertices);
								System.out.println("Elapsed time : " + (System.nanoTime() - startTime) + "ns");
								Map<String, Integer> cellCount = StorageTools.countCellsInCluster();
								long totalCount = 0;
								for (Entry<String, Integer> entry : cellCount.entrySet()) {
									System.out.println("Machine " + entry.getKey() + ": " + entry.getValue() + " cells");
									totalCount += entry.getValue();
								}	
								System.out.println("Total Count = " + totalCount);
								/*
							}
						}
						else 
							correctInput = false;
							*/
					}

					if(!correctInput)
						System.out.println("Incorrect Input");
					if(!correctRange)
						System.out.println("Incorrect Range");
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (command.equals("genEdges")) {
				try {	
					boolean correctInput = true;
					boolean correctRange = true;
					boolean directed = true;
					int numEdges = 0;
					long minId = 0;
					long maxId = 0;
					
					command = IOUtils.readLine("Number of edges: ");
					if (isNumeric(command))
						numEdges = Integer.parseInt(command);
					else 
						correctInput = false;
					/*
					if(correctInput){
						command = IOUtils.readLine("Minimum start vertex id: ");
						if (isNumeric(command))
							minId = Long.parseLong(command);
						else 
							correctInput = false;
					}
					
					if(correctInput){
						command = IOUtils.readLine("Maximum end vertex id: ");
						if (isNumeric(command)){
							maxId = Long.parseLong(command);
							if (maxId < minId)
								correctRange = false;
						}
						else 
							correctInput = false;
					}
					*/
					if(correctRange){
						if(correctInput){
							/*
							command = IOUtils.readLine("Directed (Y/N): ");
							if (command.matches("Y|N")){
								directed = command.equals("Y");
								*/
								TestTools.genEdges(1, numEdges, numEdges, false);
							/*}
							else 
								correctInput = false;
								*/
						}
						
						if(!correctInput)
							System.out.println("Incorrect Input");
					}
					else
						System.out.println("Incorrect Range");
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (command.equals("fullMesh")) {
				try {	
					boolean correctInput = true;
					boolean correctRange = true;
					int numVertices = 0;
					long minId = 1;
					long maxId = 0;
					
					command = IOUtils.readLine("Number of vertices: ");
					if (isNumeric(command)){
						numVertices = Integer.parseInt(command);
						maxId = numVertices;
					}
					else 
						correctInput = false;
					/*
					if(correctInput){
						command = IOUtils.readLine("Minimum start vertex id: ");
						if (isNumeric(command))
							minId = Long.parseLong(command);
						else 
							correctInput = false;
					}
					*/
					if(correctInput){
						/*
						command = IOUtils.readLine("Maximum end vertex id: ");
						if (isNumeric(command)){
							maxId = Long.parseLong(command);
							if ((maxId < minId) || (minId + numVertices) > maxId+1)
								correctRange = false;
							else
							*/
								TestTools.fullMesh(minId, maxId, numVertices);
							/*
						}
						else 
							correctInput = false;
							*/
					}
					
					if(!correctRange || !correctInput)
						System.out.println("Incorrect Range");
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (command.equals("getAddressesVertex")) {
				List<Long> vertexIds = new ArrayList<Long>();
				vertexIds.add(1L);
				vertexIds.add(2L);
				vertexIds.add(3L);
				vertexIds.add(4L);
				vertexIds.add(5L);
				vertexIds.add(6L);
				vertexIds.add(7L);
				vertexIds.add(8L);
				vertexIds.add(9L);
				vertexIds.add(10L);
				
				
				
				Map<String, String> clusterAddresses = StorageTools.getAddressesIps();
				ZMQ.Socket socket = null;
				ZMQ.Context context = ImgGraph.getInstance().getZMQContext();
				
				try {
					socket = context.socket(ZMQ.REQ);
					
					socket.connect("tcp://" + clusterAddresses.values().toArray()[0] + ":" + 
							Configuration.getProperty(Configuration.Key.NODE_PORT));
				
					AddressVertexReqMsg message = new AddressVertexReqMsg();
					
					message.setCellIds(vertexIds);
					
					socket.send(Message.convertMessageToBytes(message), 0);
					
					AddressVertexRepMsg response = (AddressVertexRepMsg) Message.readFromBytes(socket.recv(0));
					
					Map<Long, String> results = response.getCellAddresses();
					
					for (Entry<Long, String> e : results.entrySet()){
						System.out.println(e.getKey() + " : " + e.getValue());
					}

					socket.close();
				}finally {
					if (socket !=null)
						socket.close();
				}
				System.out.println(CacheContainer.getCellCache().getCacheManager().getAddress().toString());
				for (long i=1; i<11; i++){
					System.out.println(StorageTools.getCellAddress(i));
				}
			} else if (command.equals("move")) {
				try {			
					boolean correctInput = true;
					boolean existingVertex = true;
					boolean existingMachine = true;
					long id = 0;
					int index = 0;
					String machineName = "";
					
					//Choose which node to move
					command = IOUtils.readLine("Vertex Id : ");
					if (isNumeric(command)){
						id = Long.parseLong(command);
						if(ImgraphGraph.getInstance().getRawGraph().retrieveCell(id) == null)
							existingVertex = false;
					}
					else 
						correctInput = false;
					
					//Print Machines
					Map<String, Integer> indexMap = graph.getRawGraph().getMemberIndexes();
					for(Entry<String, Integer> entry : indexMap.entrySet()){
						System.out.println("Index :  "+entry.getValue()+"\t Address : "+ entry.getKey());
					}
					
					//Choose on which machine to move
					command = IOUtils.readLine("Machine index : ");
					if (isNumeric(command)){
						index = Integer.parseInt(command);
						for (Entry<String, Integer> entry : indexMap.entrySet()){
							if (entry.getValue() == index)
								machineName=entry.getKey();
						}
						if (machineName.equals(""))
							existingMachine = false;
					}
					else 
						correctInput = false;
					
					if(!correctInput)
						System.out.println("Incorrect Input.");
					else if(!existingVertex)
						System.out.println("The vertex " + id + " does not exist.");
					else if (!existingMachine)
						System.out.println("The machine " + index + " does not exist.");
					else {
						System.out.println("Vertex "+id+" will be moved to "+ machineName);
						
						//Save edges
						List<ImgEdge> savedEdges = ((ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(id)).getEdges();
						
						//Save attributes
						TIntObjectMap<Object> attributes = ImgraphGraph.getInstance().getRawGraph().retrieveCell(id).getAttributes();
						
						//Remove the cell
						graph.startTransaction();
						((ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(id)).remove();
						graph.stopTransaction(Conclusion.SUCCESS);
						
						//Find a free id on the target machine
						long newId = 0;
						List<Long> vertexIds = new ArrayList<Long>();
						for (long l = 0; l < id*100; l++){//TODO increase id if no free id found WTF? Rewrite that shit
							vertexIds.add(l);
						}
						
						Map<String, String> clusterAddresses = StorageTools.getAddressesIps();
						ZMQ.Socket socket = null;
						ZMQ.Context context = ImgGraph.getInstance().getZMQContext();
						
						try {
							socket = context.socket(ZMQ.REQ);
							
							socket.connect("tcp://" + clusterAddresses.values().toArray()[0] + ":" + 
									Configuration.getProperty(Configuration.Key.NODE_PORT));
						
							AddressVertexReqMsg message = new AddressVertexReqMsg();
							
							message.setCellIds(vertexIds);
							
							socket.send(Message.convertMessageToBytes(message), 0);
							
							AddressVertexRepMsg response = (AddressVertexRepMsg) Message.readFromBytes(socket.recv(0));
							
							for (Entry<Long, String> e : response.getCellAddresses().entrySet()){
								if (newId == 0 && e.getValue().equals(machineName) && ImgraphGraph.getInstance().getRawGraph().retrieveCell(e.getKey())==null)
									newId = e.getKey();
							}
							
							//Create the vertice with its edges on the target machine.
							TestTools.genVertice(newId, savedEdges, attributes);
							
							socket.close();
						}finally {
							if (socket !=null)
								socket.close();
						}
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (command.equals("remove")) {
				command  = IOUtils.readLine("Cell ID: ");
				
				if (isNumeric(command)){
						graph.startTransaction();
						ImgVertex vertex = (ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(Long.parseLong(command));
						if (vertex != null)
							vertex.remove();
						else
							System.out.println("This vertex does not exist. Please try with an other ID.");
						graph.commit();	
				}
				else 
					System.out.println("Incorrect Input");
				
			} else if (command.equals("removeEdge")) {
				command  = IOUtils.readLine("Cell ID: ");
				if (isNumeric(command)){
					graph.startTransaction();
					ImgVertex vertex = (ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(Long.parseLong(command));
					if (vertex != null){
						while (vertex.getEdges().size() != 0){
							vertex.removeEdge(vertex.getEdges().get(0));
						}
					}
					else
						System.out.println("This vertex does not exist. Please try with an other ID.");
					graph.commit();
				}
				else 
					System.out.println("Incorrect Input");
				
			}/*else if (command.equals("updateFullVector")) {
				command  = IOUtils.readLine("Cell ID: ");
				
				if (isNumeric(command)){
					NeighborhoodVector.updateFullNeighborhoodVector((ImgVertex) CacheContainer.getCellCache().get(Long.parseLong(command)));
				}
				else 
					System.out.println("Incorrect Input");
				
			} else if (command.equals("updateLocalVector")) {
				command  = IOUtils.readLine("Cell ID: ");
				
				if (isNumeric(command)){
					ImgVertex vertex = (ImgVertex) CacheContainer.getCellCache().get(Long.parseLong(command));
					vertex.setNeighborhoodVector(NeighborhoodVector.updateNeighborhoodVector(vertex));
				}
				else 
					System.out.println("Incorrect Input");
				
			}*/ else if (command.equals("getLVI")) { //LocalVertexIds
				Map<String, List<Long>> cellIds = TestTools.getCellsID();
				for(Entry<String, List<Long>> entry : cellIds.entrySet()){
					System.out.println("Machine : " + entry.getKey());
					for (Long id : entry.getValue()){
						System.out.println("\t" + id);
					}
				}
			}else if (command.equals("getCellLocation")) {
				command  = IOUtils.readLine("Cell ID: ");
				System.out.println(StorageTools.getCellAddress(Long.parseLong(command)));
			} else if (command.equals("resetCounter")) {
				nodeServer.resetCounters();
			} else if (command.equals("searchMsgCounter")) {
				System.out.println("Search messages: " + nodeServer.getSearchMsgCounter());
				
			} else if (command.equals("save")) {
				String fileName = IOUtils.readLine("File name: ");
				try {
					FileUtilities.writeToFile(fileName);
					System.out.println("OK");
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else if (command.equals("show")) {
				try {
					FileUtilities.writeD3ToFile("../data/data.json");
					System.out.println("OK");
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else if (command.equals("testSer")) {
				LoadMessage loadMsg = new LoadMessage();
				loadMsg.setLoadFileType(LoadFileType.IMGP_FILE);
				List<LoadVertexInfo> verticesInfo = new ArrayList<LoadVertexInfo>();
				LoadVertexInfo lvi = new LoadVertexInfo(2);
				lvi.addOutEdge(4);
				lvi.addInEdge(6);
				verticesInfo.add(lvi);
				
				loadMsg.setVerticesInfo(verticesInfo);
				byte[] bytes;
				try {
					bytes = Message.convertMessageToBytes(loadMsg);
					Message deSerMsg = Message.readFromBytes(bytes);
					System.out.println(loadMsg);
					System.out.println(deSerMsg);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			} else if (command.equals("genTest")) {
				graph.registerItemName("Name");
				graph.registerItemName("Size");
				graph.registerItemName("Weight");
				graph.registerItemName("Friend");
				
				graph.startTransaction();
				
				//Create vertices
				ImgVertex v25 = graph.getRawGraph().addVertex(25L, "Vertex 25");
				v25.putAttribute("Weight", 107);
				v25.putAttribute("Size", 215);
				
				ImgVertex v93 = graph.getRawGraph().addVertex(93L, "Vertex 93");
				v93.putAttribute("Weight", 81);
				v93.putAttribute("Size", 163);

				ImgVertex v66 = graph.getRawGraph().addVertex(66L, "Vertex 66");
				v66.putAttribute("Weight", 97);
				v66.putAttribute("Size", 194);
				
				ImgVertex v59 = graph.getRawGraph().addVertex(59L, "Vertex 59");
				v59.putAttribute("Weight", 66);
				v59.putAttribute("Size", 133);
				
				//Create edges
				v25.addEdge(v93, false, "Friend");
				v66.addEdge(v93, false, "Friend");
				v59.addEdge(v93, false, "Friend");
				v59.addEdge(v66, false, "Friend");
				
				graph.stopTransaction(Conclusion.SUCCESS);
				
				System.out.println("Attributes : ");
				for (String attribute : v93.getAttributeKeys()){
					System.out.println(attribute + " : " + v93.getAttribute(attribute));
				}
				
				String result = "93 is connected to {";
				List<Long> oneHop = new ArrayList<Long>();
				List<Long> twoHop = new ArrayList<Long>();
				for(ImgEdge edge : v93.getEdges()) {					//Connected To
					oneHop.add(edge.getDestCellId());
					
					for (ImgEdge twoHopEdges : ((ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(edge.getDestCellId())).getEdges()){
						if(twoHopEdges.getDestCellId() != 93){
							twoHop.add(twoHopEdges.getDestCellId());
						}
					}
				}
				System.out.println(result+"1H : "+oneHop+", 2H : "+twoHop+"}");	
								
				//Direct links -50
				System.out.println("\nDirect Links 1H : -50 attribute's values of 93 (a,b,c) :");	
				for(Long id : oneHop){
					System.out.println("93 -> " + id + ", 1H");
				}
				
				//Direct links -25
				System.out.println("\nDirect Links 2H : -25 attribute's values of 93 (D1,D2) :");
				for(Long id : twoHop){
					System.out.println("93 -> " + id + ", 2H");
				}
				
				//Undirect links -25
				System.out.println("\nUndirect Links 2H : -25 attribute's values of between them (A,B,C) :");
				List<Long> idAlreadyDone = new ArrayList<Long>();
				List<Tuple<Long, Long>> undirectLinks = new ArrayList<Tuple<Long, Long>>();
				for(Long id : oneHop){
					idAlreadyDone.add(id);
					for(Long id25 : oneHop){
						if (!idAlreadyDone.contains(id25)){
							System.out.println(id+" -> " + id25 + ", 2H");
							undirectLinks.add(new Tuple<Long, Long>(id, id25));
						}
					}
				}
				
				System.out.println("BEFORE : ");
				System.out.println(ImgGraph.getInstance().getAttributeIndex());
				
				Map<String, List<Tuple<Object, Integer>>> vector = v93.getNeighborhoodVector().getVector();
				Map<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndex = ImgGraph.getInstance().getAttributeIndex().getAttributeIndex();
				Map<String, Map<Object, List<Tuple<Long, Integer>>>> newAttributeIndex = ImgGraph.getInstance().getAttributeIndex().getAttributeIndex();
				
				for (Entry<String, Map<Object, List<Tuple<Long, Integer>>>> attributeIndexEntry : attributeIndex.entrySet()){
					
					//Check if there are values (Object) in this attribute (String) we need to remove
					if(vector.keySet().contains(attributeIndexEntry.getKey())){
						
						boolean attributeIndexEntryModified = false;
						List<Object> objectRemoved = new ArrayList<Object>();
						
						for (Entry<Object, List<Tuple<Long, Integer>>> attributeIndexEntryValue : attributeIndexEntry.getValue().entrySet()){
							
							//Check if this value (Object) is present in the removed vertex
							boolean present = false;
							for (Tuple<Object, Integer> tuple : vector.get(attributeIndexEntry.getKey())){
								if (tuple.getX().equals(attributeIndexEntryValue.getKey()))
									present = true;
							}
							
							
							boolean removed = false;
							List<Tuple<Long, Integer>> tupleRemoved = new ArrayList<Tuple<Long, Integer>>();
							if (present){
								//Search after Symmetric Direct Links
								for (String attribute : v93.getAttributeKeys()){
									if (attribute.equals(attributeIndexEntry.getKey())){
										if (v93.getAttribute(attribute).equals(attributeIndexEntryValue.getKey())){
											for (Tuple<Long, Integer> tuple : attributeIndexEntryValue.getValue()){
												if (oneHop.contains(tuple.getX()))
													tuple.setY(tuple.getY() - 50);
												if (twoHop.contains(tuple.getX()))
													tuple.setY(tuple.getY() - 25);
												if (tuple.getY() == 0)
													tupleRemoved.add(tuple);
											}
										}
										else{
											int i = 0;
											long firstId = 0;
											for (Tuple<Long, Integer> tuple : attributeIndexEntryValue.getValue()){
												if (i == 0){
													firstId = tuple.getX();
												}
												if (!tuple.getX().equals(v93.getId())){
													for (Tuple<Long, Long> undirectLinksTuple : undirectLinks){
														if(undirectLinksTuple.getX().equals(firstId) && undirectLinksTuple.getY().equals(tuple.getX()))
															tuple.setY(tuple.getY() - 25);
														else if(undirectLinksTuple.getY().equals(firstId) && undirectLinksTuple.getX().equals(tuple.getX()))
															tuple.setY(tuple.getY() - 25);
														if (tuple.getY() == 0)
															tupleRemoved.add(tuple);
													}
												}
												i++;
											}
										}
									}
								}

								//Search after possible tuple to remove
								for (Tuple<Long, Integer> tuple : attributeIndexEntryValue.getValue()){
									if (tuple.getX().equals(v93.getId())){
										tupleRemoved.add(tuple);
										removed = true;
									}
								}
							}		
							
							//Process these removals
							if (removed){
								attributeIndexEntryModified = true;
								for (Tuple<Long, Integer> tuple : tupleRemoved){
									attributeIndexEntryValue.getValue().remove(tuple);
								}
								if (attributeIndexEntryValue.getValue().size() == 0)
									objectRemoved.add(attributeIndexEntryValue.getKey());
							}
						}
						
						for (Object objectToRemove : objectRemoved)
							attributeIndexEntry.getValue().remove(objectToRemove);
						
						if (attributeIndexEntryModified){
							newAttributeIndex.put(attributeIndexEntry.getKey(), attributeIndexEntry.getValue());
						}
					}
				}
				ImgGraph.getInstance().getAttributeIndex().setAttributeIndex(newAttributeIndex);
				
				System.out.println("AFTER : ");
				System.out.println(ImgGraph.getInstance().getAttributeIndex());
			} else if (command.equals("genTestFile")) {
				try {
					String fileName = IOUtils.readLine("Output file: ");
					long minId = Long.parseLong(IOUtils.readLine("Minimum id: "));
					long maxId = Long.parseLong(IOUtils.readLine("Maximum id: "));
					int numTests = Integer.parseInt(IOUtils.readLine("Number of tests: "));
					
					if (IOUtils.readLine("Generate file containing tests with paths (Y/N): ").equals("Y")) {
						EdgeType edgeType = EdgeType.valueOf(IOUtils.readLine("Edge type (IN/OUT/UNDIRECTED): "));
						int hops = Integer.parseInt(IOUtils.readLine("Max hops: "));
						TestTools.genPathTestFile(minId, maxId, numTests, fileName, hops, edgeType);
					} else {
						TestTools.genTestFile(minId, maxId, numTests, fileName);
					}
					
					
					System.out.println("The file " + fileName + " was succesfully created");
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			} else if (command.equals("testReads")) {
				try {
					String fileName = IOUtils.readLine("Test file: ");
					Map<StatisticalIndicators, Double> statistics = TestTools.testReads(fileName, null);
					if (!statistics.isEmpty()) {
						System.out.println("Statistical indicators of processing times");
						System.out.println("Minimum: " + statistics.get(StatisticalIndicators.MIN) / 1000000);
						System.out.println("Maximum: " + statistics.get(StatisticalIndicators.MAX)/1000000);
						System.out.println("Median: " + statistics.get(StatisticalIndicators.MEDIAN)/1000000);
						System.out.println("Mean: " + statistics.get(StatisticalIndicators.MEAN)/1000000);
						System.out.println("Dev. standard: " + statistics.get(StatisticalIndicators.DEV_STD)/1000000);
						System.out.println("Total time: " + statistics.get(StatisticalIndicators.TOTAL)/1000000);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (command.equals("concurWrites")) {
				String fileName = IOUtils.readLine("Test file: ");
				int numberOfClients = Integer.parseInt(IOUtils.readLine("Number of clients: "));
				TestTools.runWriteClients(numberOfClients, fileName);
			
			} else if (command.equals("concurReads")) {
				String fileName = IOUtils.readLine("Test file: ");
				int numberOfClients = Integer.parseInt(IOUtils.readLine("Number of clients: "));
				TestTools.runReadClients(numberOfClients, fileName);
				
			} else if (command.equals("testQueryLoad")) {
				try {
					String fileName = IOUtils.readLine("Test file: ");
					command = IOUtils.readLine("Hops: ");
					int hops = Integer.parseInt(command);
					
					int numberOfClients = Integer.parseInt(IOUtils.readLine("Number of clients: "));
					long duration = Long.parseLong(IOUtils.readLine("Duration (milliseconds): "));
					
					EdgeType edgeType = EdgeType.valueOf(IOUtils.readLine("Edge type (IN|OUT|UNDIRECTED): "));
					
					TestTools.runTraversalClients(numberOfClients, fileName, hops, edgeType, duration);
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				
				
				
			} else if (command.equals("testWrites")) {
				try {
					String fileName = IOUtils.readLine("Test file: ");
					String logfile = IOUtils.readLine("Log file: (optional): ");
					Map<StatisticalIndicators, Double> statistics = TestTools.testWrites(fileName,
							(logfile.trim().equals("")?null:logfile));
					if (!statistics.isEmpty()) {
						System.out.println("Statistical indicators of processing times");
						System.out.println("Minimum: " + statistics.get(StatisticalIndicators.MIN)/1000000);
						System.out.println("Maximum: " + statistics.get(StatisticalIndicators.MAX)/1000000);
						System.out.println("Median: " + statistics.get(StatisticalIndicators.MEDIAN)/1000000);
						System.out.println("Mean: " + statistics.get(StatisticalIndicators.MEAN)/1000000);
						System.out.println("Dev. standard: " + statistics.get(StatisticalIndicators.DEV_STD)/1000000);
						System.out.println("Total time: " + statistics.get(StatisticalIndicators.TOTAL)/1000000);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (command.equals("cloneTest")) {
				graph.startTransaction();
				
				Vertex v1 = graph.addVertex(1L);
				Vertex v2  = graph.addVertex(2L);
				graph.addEdge("", v1, v2, null);
				graph.stopTransaction(Conclusion.SUCCESS);
				
				Vertex v1a = graph.getVertex(1L);
				System.out.println(v1);
				System.out.println(v1a);
				System.out.println(((ImgraphVertex)v1a).getRawVertex().getEdgeAddresses() == ((ImgraphVertex)v1).getRawVertex().getEdgeAddresses());
				((ImgraphVertex)v1).getRawVertex().addPartialEdge(4, EdgeType.OUT, "", false);
				System.out.println(v1);
				System.out.println(v1a);
				System.out.println(v1a == v1);
				
			} else if (command.equals("traverseFile")) {
				try {
					
					String fileName = IOUtils.readLine("Traversal file: ");
					String logfile = IOUtils.readLine("Log file: (optional): ");
					command = IOUtils.readLine("Hops: ");
					int hops = Integer.parseInt(command);
					command = IOUtils.readLine("Edge type (IN|OUT|UNDIRECTED): ");
					EdgeTraversalConf traversalConf =  new EdgeTraversalConf("", EdgeType.valueOf(command));
					long delay = Long.parseLong(IOUtils.readLine("Delay between traversals (ms): "));
					
					Map<StatisticalIndicators, Double> statistics = TestTools.testTraversalFromFile(graph.getRawGraph(), 
							traversalConf, Evaluation.INCLUDE_AND_STOP, fileName, hops, (logfile.trim().equals("")?null:logfile), 
							delay);
					System.out.println("File processing completed");
					
					if (!statistics.isEmpty()) {
						System.out.println("RESULTS");
						System.out.println("File: " + fileName);
						System.out.println("Max hops: " + hops + " - Edge type: " + traversalConf.getEdgeType());
						System.out.println("Delay: " + delay + " - logFile: " + logfile);
						System.out.println("Statistical indicators of processing times");
						System.out.println("Minimum: " + statistics.get(StatisticalIndicators.MIN));
						System.out.println("Maximum: " + statistics.get(StatisticalIndicators.MAX));
						System.out.println("Median: " + statistics.get(StatisticalIndicators.MEDIAN));
						System.out.println("Mean: " + statistics.get(StatisticalIndicators.MEAN));
						System.out.println("Dev. standard: " + statistics.get(StatisticalIndicators.DEV_STD));
					}
						
					
					
				} catch (Exception x) {
					x.printStackTrace();
				}
				
			} else if (command.equals("traverse")) {
				try {
					String traversalType = IOUtils.readLine("Traversal method: ");
					if (traversalType.equals("1")) {
						DistributedTraversal traversal = new DistributedTraversal();
						//Traversal traversal = new SimpleTraversal();
						
						command = IOUtils.readLine("Hops: ");
						traversal.setHops(Integer.parseInt(command));
						
						command = IOUtils.readLine("Searched Id: ");
						MatchEvaluatorConf matchConf =  new MatchEvaluatorConf();
						matchConf.setCellId(Long.parseLong(command));
						command = IOUtils.readLine("Stop at first path? (Y/N): ");
						if (command.equals("Y"))
							matchConf.setEvaluation(Evaluation.INCLUDE_AND_STOP);
						else
							matchConf.setEvaluation(Evaluation.INCLUDE_AND_CONTINUE);
						traversal.setMatchEvaluatorConf(matchConf);

						//MatchEvaluator me = new MatchEvaluator(matchConf);
						//traversal.addEvaluators(me);
						
						command = IOUtils.readLine("Edge type: ");
						EdgeTraversalConf traversalConf =  new EdgeTraversalConf("", EdgeType.valueOf(command));
						traversal.addEdgeTraversalConfs(traversalConf);
						
						command = IOUtils.readLine("Start vertex ID: ");
						traversal.setMethod(Method.BREADTH_FIRST);
						
						//ImgVertex v = (ImgVertex) graph.getRawGraph().retrieveCell(new CellId(vectorId));
						
						
						TraversalResults results = traversal.traverse((ImgVertex) graph.
								getRawGraph().retrieveCell(Long.parseLong(command)));
						
						traversal.close();
						
						if (results != null) {
						
							System.out.println("Traversal executed in " + results.getTime() + "ms");
							
							
							for (Path path : results.getPaths()) {
								System.out.println(path);
							}
						} else {
							System.out.println("There was an error processing the traversal");
						}
							
					} else if (traversalType.equals("2")) {
						SimpleTraversal traversal = new SimpleTraversal();
						command = IOUtils.readLine("Hops: ");
						traversal.setHops(Integer.parseInt(command));
						command = IOUtils.readLine("Searched Id: ");
						MatchEvaluatorConf matchConf =  new MatchEvaluatorConf();
						matchConf.setCellId(Long.parseLong(command));
						matchConf.setEvaluation(Evaluation.INCLUDE_AND_STOP);
						traversal.addEvaluators(new MatchEvaluator(matchConf));
						
						command = IOUtils.readLine("Edge type: ");
						EdgeTraversalConf traversalConf =  new EdgeTraversalConf("", EdgeType.valueOf(command));
						traversal.addEdgeTraversalConfs(traversalConf);
						
						command = IOUtils.readLine("Start vertex ID: ");
						traversal.setMethod(Method.BREADTH_FIRST);
						
						Date start =  new Date();
						TraversalResults results = traversal.traverse((ImgVertex) graph.
								getRawGraph().retrieveCell(Long.parseLong(command)));
						Date end =  new Date();
						System.out.println("Traversal executed in " + (end.getTime()-start.getTime()) + "ms");
						for (Path path : results.getPaths()) {
							System.out.println(path);
						}
						
					} 
						
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				
				
			} else if (command.equals("getIP")) {
				JGroupsAddress address = (JGroupsAddress) CacheContainer.getCacheContainer().getTransport().getAddress();
				Channel channel = ((JGroupsTransport)CacheContainer.getCacheContainer().getTransport()).getChannel();
				
				PhysicalAddress physicalAddr = (PhysicalAddress)channel.down(new Event(Event.GET_PHYSICAL_ADDRESS, address.getJGroupsAddress()));
		        if(physicalAddr instanceof IpAddress) {
		            IpAddress ipAddr = (IpAddress)physicalAddr;
		            InetAddress inetAddr = ipAddr.getIpAddress();
		            System.out.println(inetAddr.getHostAddress());
		        }
			
			} /*else if (command.equals("configCluster")){
				ClusterConfigManager configManager = new ClusterConfigManager();
				try {
					configManager.initialize();
					System.out.println("The cluster configuration was executed satisfactorily");
				} catch (Exception x) {
					x.printStackTrace();
				} finally {
					configManager.closeClientThreads();
				}
				
			} */else if (command.equals("write")){
			
				String directory = IOUtils.readLine("Directory: ");
				command = IOUtils.readLine("File name prefix: ");
				
				ImgpFileTools imgpFileTools = new ImgpFileTools("-");
				try {
					imgpFileTools.writeToFile(command, directory);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					imgpFileTools.closeClientThreads();
				}
			} else if (command.equals("countCells")) {
				try {
					Map<String, Integer> cellCount = StorageTools.countCellsInCluster();
					long totalCount = 0;
					
					for (Entry<String, Integer> entry : cellCount.entrySet()) {
						System.out.println("Machine " + entry.getKey() + ": " + entry.getValue() + " cells");
						totalCount += entry.getValue();
					}
					System.out.println("Total number of cells: " + totalCount);	
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (command.equals("clear")) {
				clearCaches();
			} else if (command.equals("exit")) {
				System.out.println("BYE...");
				Main.sendStopMessage(Configuration.getProperty(Configuration.Key.NODE_PORT));
			
				break;
			}
			/**
			 * Search
			 */
			else if (command.equals("assistedSearch")) {
				System.out.println("\nWelcome in the search assistant!\n");
				System.out.println("Attributes indexed : ");
				System.out.println(ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().keySet());
				
				String attribute = IOUtils.readLine("Select an attribute : ");
				if (ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().keySet().contains(attribute)){
					ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().get(attribute);
					System.out.println("Values indexed for the attribute " + attribute + " : ");
					System.out.println(ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().get(attribute).keySet());
					
					String value = IOUtils.readLine("Select a value : ");
					for (Entry<Object, List<Tuple<Long, Integer>>> valueIndexed : ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().get(attribute).entrySet()){
						if (valueIndexed.getKey().toString().equals(value))
							System.out.println(ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().get(attribute).get(valueIndexed.getKey()));
					}
				}
			} else if (command.equals("joinSearch")) {
				Tuple<Object, List<Tuple<Long, Integer>>> t1 = null;
				Tuple<Object, List<Tuple<Long, Integer>>> t2 = null;
				boolean found = false;
				boolean error = false;
				
				System.out.println(ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().keySet());
				String a1 = IOUtils.readLine("Select a first attribute : ");
				if (ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().keySet().contains(a1)){
					System.out.println(ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().get(a1).keySet());
					String v1 = IOUtils.readLine("Select its value : ");
					for (Entry<Object, List<Tuple<Long, Integer>>> valueIndexed : ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().get(a1).entrySet()){
						if (valueIndexed.getKey().toString().equals(v1)){
							found = true;
							t1 = new Tuple<Object, List<Tuple<Long, Integer>>>(valueIndexed.getKey(), valueIndexed.getValue());
						}
					}
					
					if (!found){
						System.out.println("Unknown value");
						error = true;
					}
				} else {
					System.out.println("Unknown attribute");
					error = true;
				}
				
				String a2 = "";
				if (!error){
					found = false;
					System.out.println(ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().keySet());
					a2 = IOUtils.readLine("Select an attribute (same or different) : ");
					if (ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().keySet().contains(a2)){
						System.out.println(ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().get(a2).keySet());
						String v2 = IOUtils.readLine("Select its value : ");
						for (Entry<Object, List<Tuple<Long, Integer>>> valueIndexed : ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().get(a2).entrySet()){
							if (valueIndexed.getKey().toString().equals(v2)){
								found = true;
								t2 = new Tuple<Object, List<Tuple<Long, Integer>>>(valueIndexed.getKey(), valueIndexed.getValue());
							}
						}
						
						if (!found){
							System.out.println("Unknown value");
							error = true;
						}
					} else {
						System.out.println("Unknown attribute");
						error = true;
					}
				}
				
				if (!error){
					System.out.println("Values indexed");
					System.out.println(a1 + " : " + t1.getX() + " : " + t1.getY());
					System.out.println(a2 + " : " + t2.getX() + " : " + t2.getY());
					/*
					Weight : 69 : [{2,50}, {3,25}, {7,25}, {1,100}]
					Size : 194 : [{2,50}, {3,100}, {7,50}, {1,25}, {4,50}, {5,50}, {6,25}]
					*/
					System.out.println("Search result : ");
					for (Tuple<Long, Integer> intensities1 : t1.getY()){
						for (Tuple<Long, Integer> intensities2 : t2.getY()){
							if(intensities1.getX().equals(intensities2.getX())){
								System.out.println("Vertex "+intensities1.getX()
										+ " ["+a1+" : {"+t1.getX()+","+intensities1.getY()+"}] "
										+"& ["+a2+" : {"+t2.getX()+","+intensities2.getY()+"}]");
							}
						}
					}
					
					
				}
				
			}else if (command.equals("joinMultipleSearch")) {
				boolean found = false;
				boolean error = false;
				int joinElements = 0;
				
				command = IOUtils.readLine("Number of attribute value to search: ");
				if (isNumeric(command))
					joinElements = Integer.parseInt(command);
				else {
					System.out.println("Incorrect Input.");
					error = true;
				}
					
				//Ask the user which {attribute, value} pairs he is looking after
				//For every pair, fill tuplesList with a list of vertices containing that pair
				//tuplesList = List<Tuple<{attribute, value}, List<Intensity of {attribute, value} in vertices containing it>>>
				List<Tuple<Tuple<String,Object>, List<Tuple<Long, Integer>>>> tuplesList = new ArrayList<Tuple<Tuple<String,Object>, List<Tuple<Long, Integer>>>>();
				
				for(int i = 0; i<joinElements; i++){
					if (!error){
						System.out.println(ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().keySet());
						String attribute = IOUtils.readLine("Select an attribute : ");
						if (ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().keySet().contains(attribute)){
							System.out.println(ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().get(attribute).keySet());
							String v1 = IOUtils.readLine("Select its value : ");
							for (Entry<Object, List<Tuple<Long, Integer>>> valueIndexed : ImgGraph.getInstance().getAttributeIndex().getAttributeIndex().get(attribute).entrySet()){
								if (valueIndexed.getKey().toString().equals(v1)){
									found = true;
									tuplesList.add(new Tuple<Tuple<String,Object>, List<Tuple<Long, Integer>>>(new Tuple<String,Object>(attribute, valueIndexed.getKey()), valueIndexed.getValue()));
								}
							}
							
							if (!found){
								System.out.println("Unknown value");
								error = true;
							}
						} else {
							System.out.println("Unknown attribute");
							error = true;
						}
					}
				}
				
				long startTime = System.nanoTime();
				
				if (!error){
					//First print the values found
					System.out.println("\nValues indexed");
					for(int i = 0; i<joinElements; i++)
						System.out.println(tuplesList.get(i).getX().getX() + " : " + tuplesList.get(i).getX().getY() + " : " + tuplesList.get(i).getY());

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
					
					//Print vertices found
					System.out.println("\nPivot values : ");
					for (Entry<Long, List<Tuple<String, Tuple<Object, Integer>>>> entry : completePivots.entrySet())
						System.out.println(entry.getKey() + " : " + entry.getValue());
					
					List<TreeSet<Long>> rawResults;
					//TODO as explained in the thesis
					//result is not empty
					if (completePivots.size() != 0){
						if (joinElements > 1)
							rawResults = buildConnectedGraph(graph, joinElements, tuplesList, new ArrayList(completePivots.keySet()), true);
						else{
							rawResults = new ArrayList<TreeSet<Long>>();
							for (Entry<Long, List<Tuple<String, Tuple<Object, Integer>>>> entry : completePivots.entrySet()){
								System.out.println(entry.getKey() + " : " + entry.getValue());
								
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
						System.out.println("Neighborhood are not overlapping, we will extend them");
						
						System.out.println("\nValues indexed");
						for(int i = 0; i<joinElements; i++)
							System.out.println(tuplesList.get(i).getX().getX() + " : " + tuplesList.get(i).getX().getY() + " : " + tuplesList.get(i).getY());
						
						List<List<Long>> expendedIds = new ArrayList<List<Long>>();
						
						//Fill expendedIds with values indexed intensities list.
						for(int i = 0; i<joinElements; i++){
							List<Long> ids = new ArrayList<Long>();
							for (Tuple<Long, Integer> tuple : tuplesList.get(i).getY())
								ids.add(tuple.getX());
							
							expendedIds.add(ids);
						}
						
						System.out.println("Expended Ids : " + expendedIds);
						
						List<Long> pivots = updatePivots(expendedIds);
						
						System.out.println("Pivots : " + pivots);
						if (joinElements > 1){
							while (checkPivotInAllLists(pivots, expendedIds) == null){ //null if no pivots present in all Lists
								int id = getListIdNoPivots(pivots, expendedIds);//id of a list containing no pivots
								
								if (id == -1){
									id = getListIdLessPivots(pivots, expendedIds);//id of a list containing less pivots than other lists
								}
								
								List<Long> expendedList = expend(expendedIds.get(id));
								expendedIds.remove(id);
								expendedIds.add(expendedList);
								
								pivots = updatePivots(expendedIds);
								
							}
						}
						
						
						System.out.println("Expended Ids : " + expendedIds);
						System.out.println("Pivots : " + pivots);
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
						System.out.println("Final pivots : " + expandedCompletePivots);
						

						rawResults = buildConnectedGraph(graph, joinElements, tuplesList, expandedCompletePivots, false);
					}
					
					//REMOVE RESULTS BIGGER THAN OTHERS
					boolean modified = true;
					
					while (modified){
						modified = false;
						List<TreeSet<Long>> toRemove = new ArrayList<TreeSet<Long>>();
						if (rawResults.size() != 0){
							int size = rawResults.get(0).size();
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

					//PRINT FINAL RESULTS
					if (rawResults.size() < 2)
						System.out.println("\nSearch result : "+rawResults);
					else
						System.out.println("\nSearch results : "+rawResults);
				}
				
				System.out.println("Elapsed time : " + (System.nanoTime() - startTime) + "ns");
				
				
			} else if (command.equals("manualJoinSearch")) {
				//Get user inputs
				boolean found = false;
				boolean error = false;
				int joinElements = 0;
				List<Tuple<String,String>> userInput = new ArrayList<Tuple<String,String>>();
				List<Tuple<Tuple<String,Object>, List<Tuple<Long, Integer>>>> tuplesList = new ArrayList<Tuple<Tuple<String,Object>, List<Tuple<Long, Integer>>>>();
				
				command = IOUtils.readLine("Number of attribute value to search: ");
				if (isNumeric(command))
					joinElements = Integer.parseInt(command);
				else {
					System.out.println("Incorrect Input.");
					error = true;
				}
				
				for(int i = 0; i<joinElements; i++){
					if (!error){
						String attribute = IOUtils.readLine("Select an attribute : ");
						String value = IOUtils.readLine("Select its value : ");
						
						userInput.add(new Tuple<String, String>(attribute, value));
						
						//Initialization of tupleList
						tuplesList.add(new Tuple<Tuple<String,Object>,List<Tuple<Long, Integer>>>(
											new Tuple<String, Object>(attribute, value),
											new ArrayList<Tuple<Long, Integer>>()));
					}
				}
				
				long startTime = System.nanoTime();
				
				//Check, for each vertex, if it contains the researched attributes
				for (Entry<String, List<Long>> machineList : TestTools.getCellsID().entrySet()){
					for (Long id : machineList.getValue()){
						ImgVertex vertex = graph.getRawGraph().getVertex(id);
						
						//Get its map of attribute-value
						final Map<String, Object> attributeMap = new HashMap<String, Object>(); 
						if (vertex != null){
							if (vertex.getAttributes() != null && !vertex.getAttributes().isEmpty()) {
								final ImgGraph g = ImgGraph.getInstance();
								vertex.getAttributes().forEachEntry(new TIntObjectProcedure<Object>() {
									@Override
									public boolean execute(int keyIndex, Object value) {
										attributeMap.put(g.getItemName(keyIndex), value);
										return true;
									}
								});
							}
						}
						
						//Check, for each vertex, if it contains the researched attributes
						for (Tuple<String,String> researchedValue : userInput){
							Object value = attributeMap.get(researchedValue.getX());
							if (value != null && value.toString().equals(researchedValue.getY())){
								//This vertex contains a researched attribute value
								//Need to add it to tupleList
								for (Tuple<Tuple<String,Object>, List<Tuple<Long, Integer>>> tuple : tuplesList){
									if (tuple.getX().getX().equals(researchedValue.getX())
											&& tuple.getX().getY().equals(researchedValue.getY())){
										tuple.getY().add(new Tuple<Long, Integer>(id, 100));
									}
								}
							}
						}
					}
				}
				
				//Do the normal search process.
				if (!error){
					//First print the values found
					System.out.println("\nValues researched");
					for(int i = 0; i<joinElements; i++)
						System.out.println(tuplesList.get(i).getX().getX() + " : " + tuplesList.get(i).getX().getY() + " : " + tuplesList.get(i).getY());

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
					
					//Fill expendedIds with values indexed intensities list.
					for(int i = 0; i<joinElements; i++){
						List<Long> ids = new ArrayList<Long>();
						for (Tuple<Long, Integer> tuple : tuplesList.get(i).getY())
							ids.add(tuple.getX());
						
						expendedIds.add(ids);
					}
					
					List<Long> pivots = updatePivots(expendedIds);
					
					if (joinElements > 1){
						while (checkPivotInAllLists(pivots, expendedIds) == null){ //null if no pivots present in all Lists
							int id = getListIdNoPivots(pivots, expendedIds);//id of a list containing no pivots
							
							if (id == -1){
								id = getListIdLessPivots(pivots, expendedIds);//id of a list containing less pivots than other lists
							}
							
							List<Long> expendedList = expend(expendedIds.get(id));
							expendedIds.remove(id);
							expendedIds.add(expendedList);
							
							pivots = updatePivots(expendedIds);
							
						}
					}
					
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
					System.out.println("Complete pivots : "+finalPivots);
					System.out.println("ExpendedIds : "+expendedIds);

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
								
								if (copy.size()!=0 && checkConnected(copy) && checkValuesPresent(joinElements, tuplesList, copy)){
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
					modified = true;
					
					while (modified){
						modified = false;
						List<TreeSet<Long>> toRemove = new ArrayList<TreeSet<Long>>();
						if (rawResults.size() != 0){
							int size = rawResults.get(0).size();
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

					//PRINT FINAL RESULTS
					if (rawResults.size() < 2)
						System.out.println("\nSearch result : "+rawResults);
					else
						System.out.println("\nSearch results : "+rawResults);
				}
				
				System.out.println("Elapsed time : " + (System.nanoTime() - startTime) + "ns");
				
			} else if (command.equals("searchTestLimits")) {
				graph.registerItemName("Name");
				graph.registerItemName("Size");
				graph.registerItemName("Weight");
				graph.registerItemName("Friend");
				
				graph.startTransaction();
				
				//Create vertices
				ImgVertex v1 = graph.getRawGraph().addVertex(1L, "Vertex 1");
				v1.putAttribute("Weight", 100);
				v1.putAttribute("Size", 100);
				
				ImgVertex v2 = graph.getRawGraph().addVertex(2L, "Vertex 2");
				
				ImgVertex v3 = graph.getRawGraph().addVertex(3L, "Vertex 3");
				
				ImgVertex v4 = graph.getRawGraph().addVertex(4L, "Vertex 4");
				
				ImgVertex v5 = graph.getRawGraph().addVertex(5L, "Vertex 5");
				
				ImgVertex v6 = graph.getRawGraph().addVertex(6L, "Vertex 6");
				
				ImgVertex v7 = graph.getRawGraph().addVertex(7L, "Vertex 7");
				
				ImgVertex v8 = graph.getRawGraph().addVertex(8L, "Vertex 8");
				
				ImgVertex v9 = graph.getRawGraph().addVertex(9L, "Vertex 9");
				
				ImgVertex v10 = graph.getRawGraph().addVertex(10L, "Vertex 10");
				v10.putAttribute("Weight", 200);
				v10.putAttribute("Size", 200);
				
				//Create edges
				v1.addEdge(v2, false, "Friend");
				v2.addEdge(v3, false, "Friend");
				v3.addEdge(v4, false, "Friend");
				v4.addEdge(v5, false, "Friend");
				v4.addEdge(v6, false, "Friend");
				v5.addEdge(v7, false, "Friend");
				v6.addEdge(v7, false, "Friend");
				v7.addEdge(v8, false, "Friend");
				v8.addEdge(v9, false, "Friend");
				v9.addEdge(v10, false, "Friend");
				
				graph.stopTransaction(Conclusion.SUCCESS);
			} else if (command.equals("searchTest")) {
				graph.registerItemName("Name");
				graph.registerItemName("Size");
				graph.registerItemName("Weight");
				graph.registerItemName("Friend");
				
				graph.startTransaction();
				
				//Create vertices
				ImgVertex v1 = graph.getRawGraph().addVertex(1L, "Vertex 1");
				v1.putAttribute("Weight", 69);
				v1.putAttribute("Size", 215);
				
				ImgVertex v2 = graph.getRawGraph().addVertex(2L, "Vertex 2");
				v2.putAttribute("Weight", 81);
				v2.putAttribute("Size", 163);

				ImgVertex v3 = graph.getRawGraph().addVertex(3L, "Vertex 3");
				v3.putAttribute("Weight", 97);
				v3.putAttribute("Size", 194);
				
				ImgVertex v4 = graph.getRawGraph().addVertex(4L, "Vertex 4");
				v4.putAttribute("Weight", 66);
				v4.putAttribute("Size", 133);
				
				ImgVertex v5 = graph.getRawGraph().addVertex(5L, "Vertex 5");
				v5.putAttribute("Weight", 107);
				v5.putAttribute("Size", 215);
				
				ImgVertex v6 = graph.getRawGraph().addVertex(6L, "Vertex 6");
				v6.putAttribute("Weight", 75);
				v6.putAttribute("Size", 163);

				ImgVertex v7 = graph.getRawGraph().addVertex(7L, "Vertex 7");
				v7.putAttribute("Weight", 93);
				v7.putAttribute("Size", 192);
				
				ImgVertex v8 = graph.getRawGraph().addVertex(8L, "Vertex 8");
				v8.putAttribute("Weight", 85);
				v8.putAttribute("Size", 185);
				
				//Create edges
				v1.addEdge(v2, false, "Friend");
				v2.addEdge(v3, false, "Friend");
				v2.addEdge(v7, false, "Friend");
				v3.addEdge(v4, false, "Friend");
				v3.addEdge(v5, false, "Friend");
				v5.addEdge(v6, false, "Friend");
				v5.addEdge(v7, false, "Friend");
				v7.addEdge(v8, false, "Friend");
				
				graph.stopTransaction(Conclusion.SUCCESS);
			} else if (command.equals("searchTest1")) {
				graph.registerItemName("Name");
				graph.registerItemName("City Name");
				graph.registerItemName("State");
				graph.registerItemName("Friend");
				
				graph.startTransaction();
				
				//Create vertices
				ImgVertex v1 = graph.getRawGraph().addVertex(1L, "Vertex 1");
				v1.putAttribute("State", "California");
				v1.putAttribute("City Name", "Beverly Hills");
				
				ImgVertex v2 = graph.getRawGraph().addVertex(2L, "Vertex 2");
				v2.putAttribute("State", "Arizona");
				v2.putAttribute("City Name", "Phoenix");
				
				ImgVertex v3 = graph.getRawGraph().addVertex(3L, "Vertex 3");
				
				ImgVertex v4 = graph.getRawGraph().addVertex(4L, "Vertex 4");
				
				ImgVertex v5 = graph.getRawGraph().addVertex(5L, "Vertex 5");
				
				ImgVertex v6 = graph.getRawGraph().addVertex(6L, "Vertex 6");
				v6.putAttribute("State", "Texas");
				v6.putAttribute("City Name", "Beverly Hills");
				
				//Create edges
				v1.addEdge(v2, false, "Friend");
				v2.addEdge(v3, false, "Friend");
				v2.addEdge(v4, false, "Friend");
				v3.addEdge(v5, false, "Friend");
				v4.addEdge(v5, false, "Friend");
				v5.addEdge(v6, false, "Friend");
				
				graph.stopTransaction(Conclusion.SUCCESS);
			} else if (command.equals("searchTest1bis")) {
				graph.registerItemName("Name");
				graph.registerItemName("City Name");
				graph.registerItemName("State");
				graph.registerItemName("Friend");
				
				graph.startTransaction();
				
				//Create vertices
				ImgVertex v1 = graph.getRawGraph().addVertex(1L, "Vertex 1");
				v1.putAttribute("State", "California");
				v1.putAttribute("City Name", "Beverly Hills");
				
				ImgVertex v2 = graph.getRawGraph().addVertex(2L, "Vertex 2");
				v2.putAttribute("State", "Arizona");
				v2.putAttribute("City Name", "Phoenix");
				
				ImgVertex v3 = graph.getRawGraph().addVertex(3L, "Vertex 3");
				
				ImgVertex v4 = graph.getRawGraph().addVertex(4L, "Vertex 4");
				
				ImgVertex v5 = graph.getRawGraph().addVertex(5L, "Vertex 5");
				
				ImgVertex v6 = graph.getRawGraph().addVertex(6L, "Vertex 6");
				
				ImgVertex v7 = graph.getRawGraph().addVertex(7L, "Vertex 7");
				v7.putAttribute("State", "Texas");
				v7.putAttribute("City Name", "Beverly Hills");
				
				//Create edges
				v1.addEdge(v2, false, "Friend");
				v2.addEdge(v3, false, "Friend");
				v2.addEdge(v4, false, "Friend");
				v3.addEdge(v5, false, "Friend");
				v4.addEdge(v5, false, "Friend");
				v5.addEdge(v6, false, "Friend");
				v6.addEdge(v7, false, "Friend");
				
				graph.stopTransaction(Conclusion.SUCCESS);
			} else if (command.equals("searchTest2")) {
				graph.registerItemName("Name");
				graph.registerItemName("Size");
				graph.registerItemName("Weight");
				graph.registerItemName("Friend");
				
				graph.startTransaction();
				
				//Create vertices
				ImgVertex v1 = graph.getRawGraph().addVertex(1L, "Vertex 1");
				v1.putAttribute("Weight", 69);
				v1.putAttribute("Size", 215);
				
				ImgVertex v2 = graph.getRawGraph().addVertex(2L, "Vertex 2");
				v2.putAttribute("Weight", 81);
				v2.putAttribute("Size", 163);

				ImgVertex v3 = graph.getRawGraph().addVertex(3L, "Vertex 3");
				v3.putAttribute("Weight", 97);
				v3.putAttribute("Size", 194);
				
				ImgVertex v4 = graph.getRawGraph().addVertex(4L, "Vertex 4");
				v4.putAttribute("Weight", 66);
				v4.putAttribute("Size", 133);
				
				ImgVertex v5 = graph.getRawGraph().addVertex(5L, "Vertex 5");
				v5.putAttribute("Weight", 107);
				v5.putAttribute("Size", 215);
				
				ImgVertex v6 = graph.getRawGraph().addVertex(6L, "Vertex 6");
				v6.putAttribute("Weight", 75);
				v6.putAttribute("Size", 163);

				ImgVertex v7 = graph.getRawGraph().addVertex(7L, "Vertex 7");
				v7.putAttribute("Weight", 93);
				v7.putAttribute("Size", 192);
				
				ImgVertex v8 = graph.getRawGraph().addVertex(8L, "Vertex 8");
				v8.putAttribute("Weight", 85);
				v8.putAttribute("Size", 185);
				
				ImgVertex v9 = graph.getRawGraph().addVertex(9L, "Vertex 9");
				v9.putAttribute("Weight", 77);
				v9.putAttribute("Size", 154);
				
				ImgVertex v10 = graph.getRawGraph().addVertex(10L, "Vertex 10");
				v10.putAttribute("Weight", 72);
				v10.putAttribute("Size", 144);
				
				ImgVertex v11 = graph.getRawGraph().addVertex(11L, "Vertex 11");
				v11.putAttribute("Weight", 22);
				v11.putAttribute("Size", 200);
				
				ImgVertex v12 = graph.getRawGraph().addVertex(12L, "Vertex 12");
				v12.putAttribute("Weight", 55);
				v12.putAttribute("Size", 124);

				ImgVertex v13 = graph.getRawGraph().addVertex(13L, "Vertex 13");
				v13.putAttribute("Weight", 36);
				v13.putAttribute("Size", 124);
				
				ImgVertex v14 = graph.getRawGraph().addVertex(14L, "Vertex 14");
				v14.putAttribute("Weight", 57);
				v14.putAttribute("Size", 145);
				
				ImgVertex v15 = graph.getRawGraph().addVertex(15L, "Vertex 15");
				v15.putAttribute("Weight", 69);
				v15.putAttribute("Size", 168);
				
				ImgVertex v16 = graph.getRawGraph().addVertex(16L, "Vertex 16");
				v16.putAttribute("Weight", 39);
				v16.putAttribute("Size", 100);

				ImgVertex v17 = graph.getRawGraph().addVertex(17L, "Vertex 17");
				v17.putAttribute("Weight", 79);
				v17.putAttribute("Size", 171);
				
				ImgVertex v18 = graph.getRawGraph().addVertex(18L, "Vertex 18");
				v18.putAttribute("Weight", 70);
				v18.putAttribute("Size", 143);
				
				ImgVertex v19 = graph.getRawGraph().addVertex(19L, "Vertex 19");
				v19.putAttribute("Weight", 71);
				v19.putAttribute("Size", 150);
				
				ImgVertex v20 = graph.getRawGraph().addVertex(20L, "Vertex 20");
				v20.putAttribute("Weight", 73);
				v20.putAttribute("Size", 146);
				
				//Create edges
				v1.addEdge(v2, false, "Friend");
				v2.addEdge(v3, false, "Friend");
				v3.addEdge(v4, false, "Friend");
				v4.addEdge(v5, false, "Friend");
				v5.addEdge(v6, false, "Friend");
				v6.addEdge(v7, false, "Friend");
				v7.addEdge(v8, false, "Friend");
				v8.addEdge(v9, false, "Friend");
				v9.addEdge(v10, false, "Friend");
				
				v11.addEdge(v12, false, "Friend");
				v12.addEdge(v13, false, "Friend");
				v13.addEdge(v14, false, "Friend");
				v14.addEdge(v15, false, "Friend");
				v15.addEdge(v16, false, "Friend");
				v16.addEdge(v17, false, "Friend");
				v17.addEdge(v18, false, "Friend");
				v18.addEdge(v19, false, "Friend");
				v19.addEdge(v20, false, "Friend");
				
				v1.addEdge(v11, false, "Friend");
				v2.addEdge(v12, false, "Friend");
				v3.addEdge(v13, false, "Friend");
				v4.addEdge(v14, false, "Friend");
				v5.addEdge(v15, false, "Friend");
				v6.addEdge(v16, false, "Friend");
				v7.addEdge(v17, false, "Friend");
				v8.addEdge(v18, false, "Friend");
				v9.addEdge(v19, false, "Friend");
				v10.addEdge(v20, false, "Friend");
				
				graph.stopTransaction(Conclusion.SUCCESS);
			} else if (command.equals("searchTest3")) {
				graph.registerItemName("Name");
				graph.registerItemName("Weight");
				graph.registerItemName("Size");
				graph.registerItemName("Friend");
				
				graph.startTransaction();
				
				//Create vertices
				ImgVertex v1 = graph.getRawGraph().addVertex(1L, "Vertex 1");
				v1.putAttribute("Size", 185);
				
				ImgVertex v2 = graph.getRawGraph().addVertex(2L, "Vertex 2");
				v2.putAttribute("Weight", 72);
				
				ImgVertex v3 = graph.getRawGraph().addVertex(3L, "Vertex 3");
				v3.putAttribute("Weight", 85);
				v3.putAttribute("Size", 185);
				
				ImgVertex v4 = graph.getRawGraph().addVertex(4L, "Vertex 4");
				
				ImgVertex v5 = graph.getRawGraph().addVertex(5L, "Vertex 5");
				
				ImgVertex v6 = graph.getRawGraph().addVertex(6L, "Vertex 6");
				v6.putAttribute("Size", 176);

				ImgVertex v7 = graph.getRawGraph().addVertex(7L, "Vertex 7");
				
				ImgVertex v8 = graph.getRawGraph().addVertex(8L, "Vertex 8");
				//v8.putAttribute("Size", 169);
				
				ImgVertex v9 = graph.getRawGraph().addVertex(9L, "Vertex 9");
				
				//Create edges
				v1.addEdge(v2, false, "Friend");
				v2.addEdge(v3, false, "Friend");
				v3.addEdge(v4, false, "Friend");
				v4.addEdge(v5, false, "Friend");
				v6.addEdge(v7, false, "Friend");
				v7.addEdge(v3, false, "Friend");
				v9.addEdge(v8, false, "Friend");
				v8.addEdge(v3, false, "Friend");
				
				graph.stopTransaction(Conclusion.SUCCESS);
				
				graph.startTransaction();
				ImgVertex v10 = graph.getRawGraph().getVertex(1L);
				v10.putAttribute("Size", "200");
				
				ImgVertex v30 = graph.getRawGraph().getVertex(3L);
				v30.removeAttribute("Weight");
				
				graph.stopTransaction(Conclusion.SUCCESS);
				
			} else if (command.equals("searchTest4")) {
				graph.registerItemName("Name");
				graph.registerItemName("Weight");
				graph.registerItemName("Size");
				graph.registerItemName("Friend");
				
				graph.startTransaction();
				
				//Create vertices
				ImgVertex v1 = graph.getRawGraph().addVertex(1L, "Vertex 1");
				v1.putAttribute("Weight", 82);
				v1.putAttribute("Size", 165);
				
				ImgVertex v2 = graph.getRawGraph().addVertex(2L, "Vertex 2");
				v2.putAttribute("Weight", 81);
				v2.putAttribute("Size", 162);
				
				ImgVertex v3 = graph.getRawGraph().addVertex(3L, "Vertex 3");
				v3.putAttribute("Weight", 60);
				v3.putAttribute("Size", 120);
				graph.stopTransaction(Conclusion.SUCCESS);
				
				
				//Create edges
				graph.startTransaction();
				graph.getRawGraph().getVertex(2L).addEdge(graph.getRawGraph().getVertex(3L), false, "Friend");
				graph.stopTransaction(Conclusion.SUCCESS);
				
				graph.startTransaction();
				graph.getRawGraph().getVertex(1L).addEdge(graph.getRawGraph().getVertex(3L), false, "Friend");
				graph.stopTransaction(Conclusion.SUCCESS);
				
				graph.startTransaction();
				graph.getRawGraph().getVertex(1L).addEdge(graph.getRawGraph().getVertex(2L), false, "Friend");
				graph.stopTransaction(Conclusion.SUCCESS);
				
				
				
				graph.startTransaction();
				ImgVertex v4 = graph.getRawGraph().addVertex(4L, "Vertex 4");
				v4.putAttribute("Weight", 93);
				v4.putAttribute("Size", 187);
				graph.stopTransaction(Conclusion.SUCCESS);
				
				
				graph.startTransaction();
				graph.getRawGraph().getVertex(3L).addEdge(graph.getRawGraph().getVertex(4L), false, "Friend");
				graph.stopTransaction(Conclusion.SUCCESS);
				
				graph.startTransaction();
				graph.getRawGraph().getVertex(1L).addEdge(graph.getRawGraph().getVertex(4L), false, "Friend");
				graph.stopTransaction(Conclusion.SUCCESS);
			} 
			/**
			 * Actors TODO
			 */
			//Simple chat using actors
			else if (command.equals("StartChat")){
				String ip = "";
				Map<String, String> ipsMap = StorageTools.getAddressesIps();
				for(Entry<String, String> entry : ipsMap.entrySet()){
					if (CacheContainer.getCellCache().getCacheManager().getAddress().toString().equals(entry.getKey()))
						ip = entry.getValue();
				}
				
				Config config = ConfigFactory.parseString("akka {actor {"
						+ "provider = \"akka.remote.RemoteActorRefProvider\"},"
						+ "remote {netty {"
							+ "hostname = \""+ip+"\","
							+ "port = 5679}}}");
				
				system = ActorSystem.create("ChatDaemon", config);
				chat = system.actorOf(new Props(ChatActor.class), "chat");
			} else if (command.equals("chat")){
				chat.tell(new Tuple<String, String>("10.211.55.3:5679", "TEST 1")); //TODO Replace by a dynamic ip/message 
			} else if (command.equals("chat2")){
				chat.tell(new Tuple<String, String>("10.211.55.5:5679", "test 2"));
			}
			/**
			 * Trigger
			 */
			else if (command.equals("indexOn")){
				NeighborhoodVector.indexEnabled = true;
			}
			else if (command.equals("indexOff")){
				NeighborhoodVector.indexEnabled = false;
			}
			else if (command.equals("actorsOn")){
				NeighborhoodVector.actorsEnabled = true;
			}
			else if (command.equals("actorsOff")){
				NeighborhoodVector.actorsEnabled = false;
			}
			else if (command.equals("runGGTests")){ //generates graphs (x vertices & y edges)
				clearCaches();
				NeighborhoodVector.testing = true;
				long minId = 1;
				
				command = IOUtils.readLine("Number of machines: ");
				NeighborhoodVector.numberOfMachines = Integer.parseInt(command);
				
				command = IOUtils.readLine("Number of vertices: ");
				long numVertices = Long.parseLong(command);
				
				command = IOUtils.readLine("Number of edges: ");
				int numEdges = Integer.parseInt(command);
				
				long maxId = numVertices;
				
				long startTime;
				long totalTime;
				
				//1
				NeighborhoodVector.indexEnabled = false;
				NeighborhoodVector.actorsEnabled = false;
				
				System.out.println("\n\nRound 1 :");
				totalTime = 0;
				for (int i=0; i<10; i++){
					startTime = System.nanoTime();
					TestTools.genVertices(minId, maxId, numVertices);
					if (numEdges > 0)
						TestTools.genEdges(minId, maxId, numEdges, false);
					System.out.println("Elapsed time for turn "+(i+1)+" : " + (System.nanoTime() - startTime) + "ns");
					totalTime += (System.nanoTime() - startTime);
					clearCaches();
				}	
				System.out.println("Total time for round 1 : " + totalTime + "ns");
				System.out.println("Average time for round 1 : " + (totalTime/10) + "ns");
				
				//2
				NeighborhoodVector.indexEnabled = true;
				NeighborhoodVector.actorsEnabled = true;
				
				System.out.println("\n\nRound 2 :");
				totalTime = 0;
				for (int i=0; i<10; i++){
					startTime = System.nanoTime();
					TestTools.genVertices(minId, maxId, numVertices);
					if (numEdges > 0)
						TestTools.genEdges(minId, maxId, numEdges, false);
					System.out.println("Elapsed time for turn "+(i+1)+" : " + (System.nanoTime() - startTime) + "ns");
					totalTime += (System.nanoTime() - startTime);
					clearCaches();
				}	
				System.out.println("Total time for round 2 : " + totalTime + "ns");
				System.out.println("Average time for round 2 : " + (totalTime/10) + "ns");
				
				//3
				NeighborhoodVector.indexEnabled = true;
				NeighborhoodVector.actorsEnabled = false;

				System.out.println("\n\nRound 3 :");
				totalTime = 0;
				for (int i=0; i<10; i++){
					startTime = System.nanoTime();
					TestTools.genVertices(minId, maxId, numVertices);
					if (numEdges > 0)
						TestTools.genEdges(minId, maxId, numEdges, false);
					System.out.println("Elapsed time for turn "+(i+1)+" : " + (System.nanoTime() - startTime) + "ns");
					totalTime += (System.nanoTime() - startTime);
					clearCaches();
				}	
				System.out.println("Total time for round 3 : " + totalTime + "ns");
				System.out.println("Average time for round 3 : " + (totalTime/10) + "ns");
				
				NeighborhoodVector.testing = false;
			}
		}
	}

	private static void clearCaches() {
		CacheContainer.getCellCache().clear();
		//Clear Attribute Index
		ImgGraph.getInstance().setAttributeIndex(new AttributeIndex());
		
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
					
					Message requestMessage = new Message(MessageType.CLEAR_ATTRIBUTE_INDEX_REQ);
					
					socket.send(Message.convertMessageToBytes(requestMessage), 0);
					
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

	private static List<TreeSet<Long>> buildConnectedGraph(
			ImgraphGraph graph,
			int joinElements,
			List<Tuple<Tuple<String, Object>, List<Tuple<Long, Integer>>>> tuplesList,
			List<Long> completePivots,
			boolean onlyInNeighborhood) {
		boolean found;
		List<TreeSet<Long>> rawResults = new ArrayList<TreeSet<Long>>();
		for (Long pivotID : completePivots){
			//Find a connected graph for every entry in result
			TreeSet<Long> connectedGraph = new TreeSet<Long>();
			List<Tuple<String,Object>> valueFound = new ArrayList<Tuple<String,Object>>();
			
			//First add this vertice
			ImgVertex vertex = (ImgVertex) graph.getRawGraph().retrieveCell(pivotID);
			connectedGraph.add(vertex.getId());
			
			//Check if we found a value in this vertex
			found = checkIfNewValueFound(joinElements, tuplesList, valueFound, vertex);
			
			
			//We found maybe the last value needed
			boolean allFound = false;
			if (found) //TODO
				allFound = allValuesFound(joinElements, tuplesList, valueFound);
			//All researched values are in this vertex
			if (!found || !allFound){
				//Need to expand the connectedGraph
				//TODO not anymore Only add vertices that are in neighborhood of researched values 
				boolean stop = false;
				while(!stop){
					List<Long> newIds = new ArrayList<Long>();
					for (Long id : connectedGraph){
						vertex = (ImgVertex) graph.getRawGraph().retrieveCell(id);
						for (ImgEdge edge : vertex.getEdges()){
							if (!connectedGraph.contains(edge.getDestCellId())){
								
								ImgVertex newVertex = (ImgVertex) graph.getRawGraph().retrieveCell(edge.getDestCellId());
								
								if (!onlyInNeighborhood || checkIfValuePresentInNeighborhoodVector(joinElements, tuplesList, newVertex)){
									newIds.add(newVertex.getId());
									
									//Check if we found a value in this vertex
									found = checkIfNewValueFound(joinElements, tuplesList, valueFound, newVertex);
									//We found maybe the last value needed
									allFound = allValuesFound(joinElements, tuplesList, valueFound);
									//All researched values are in this vertex
									if (found && allFound)
										stop = true;
								}
							}
						}
					}
					
					for (Long newId : newIds){
						if (!connectedGraph.contains(newId))
								connectedGraph.add(newId);
					}
				}
			}

			//PRUNNING
			//Removes vertices that don't take away these graph properties :
			//	-The graph must be connected : checkConnected
			//	-All researched values must be in the graph : checkValuesPresent
			
			boolean modified = true;
			
			while (modified){
				modified = false;
				List<Long> copy = new ArrayList<Long>(connectedGraph);
				List<Long> removedIds = new ArrayList<Long>();
				
				for (Long id : connectedGraph){
					copy.remove(id);
					
					if (copy.size()!=0 
							&& copy.contains(pivotID) 
							&& checkConnected(copy) 
							&& checkValuesPresent(joinElements, tuplesList, copy)){
						modified = true;
						removedIds.add(id);
					}
					else
						copy.add(id);
				}
				for(Long id : removedIds)
					connectedGraph.remove(id);
			}
			
			//Avoid duplicates
			if (!rawResults.contains(connectedGraph))
				rawResults.add(connectedGraph);

		}
		return rawResults;
	}

	private static List<Long> expend(List<Long> list) {
		List<Long> result = new ArrayList<Long>();
		for(Long id : list){
			if (!result.contains(id))
				result.add(id);

			ImgVertex vertex = (ImgVertex) ImgGraph.getInstance().retrieveCell(id);
			
			for (ImgEdge edge : vertex.getEdges()){
				if (!result.contains(edge.getDestCellId()))
					result.add(edge.getDestCellId());
			}
			
		}
		return result;
	}

	

	private static List<Long> updatePivots(List<List<Long>> expendedIds) {
		List<Long> pivots = new ArrayList<Long>();
		
		for (int i = 0; i < expendedIds.size(); i++){
			List<Long> current = expendedIds.get(i);
			for (List<Long> list : expendedIds){
				if (!list.equals(current)){
					for (Long id : list){
						if (current.contains(id) && !pivots.contains(id))
							pivots.add(id);
					}
				}
			}
		}
		return pivots;
	}
	
	private static int getListIdLessPivots(List<Long> pivots, List<List<Long>> expendedIds) {
		int result = 0;
		int min = 0;
		int tmp = 0;
		//INIT min value
		for (Long pivot : pivots){
			if (expendedIds.get(0).contains(pivot))
				min++;
		}

		for (int i = 0; i < expendedIds.size(); i++){
			tmp = 0;
			for (Long pivot : pivots){
				if (expendedIds.get(i).contains(pivot))
					tmp++;
			}
			if (tmp < min){
				min = tmp;
				result = i;
			}
		}
		
		return result;
	}

	private static int getListIdNoPivots(List<Long> pivots, List<List<Long>> expendedIds) {
		int result = -1;
		boolean present;
		for (int i = 0; i < expendedIds.size(); i++){
			present = false;
			for (Long pivot : pivots){
				if (expendedIds.get(i).contains(pivot))
					present = true;
			}
			if (!present)
				result = i;
		}
		
		return result;
	}

	private static Long checkPivotInAllLists(List<Long> pivots, List<List<Long>> expendedIds) {
		Long result = null;
		boolean presentInAll = true;
		
		for (Long id : pivots){
			presentInAll = true;
			for (List<Long> list : expendedIds){
				if (!list.contains(id))
					presentInAll = false;
			}
			if (presentInAll)
				result = id;
		}
		
		return result;
	}

	private static boolean checkValuesPresent(int joinElements,
			List<Tuple<	Tuple<String, Object>, 
						List<Tuple<Long, Integer>>>> tuplesList, 
			List<Long> connectedGraph) {
		boolean result = true;
		
		for(int i = 0; i<joinElements; i++){
			String attribute = tuplesList.get(i).getX().getX();
			Object value = tuplesList.get(i).getX().getY() ;
			
			boolean valuePresent = false;
			
			for (Long id : connectedGraph){
				ImgVertex vertex = (ImgVertex) ImgGraph.getInstance().retrieveCell(id);
				
				Object vertexValue = vertex.getAttribute(attribute);
				
				if (vertexValue != null && vertexValue.toString().equals(value.toString()))
					valuePresent = true;
			}
			
			if (!valuePresent)
				result = false;
		}
		
		return result;
	}

	private static boolean checkConnected(List<Long> connectedGraph) {
		List<Long> checked = new ArrayList<Long>();
		
		//Add the first vertex
		ImgVertex vertex = (ImgVertex) ImgGraph.getInstance().retrieveCell(connectedGraph.get(0));
		checked.add(vertex.getId());
		
		boolean modified = true;
		while(modified){
			modified = false;
			for (Long id : connectedGraph){
				vertex = (ImgVertex) ImgGraph.getInstance().retrieveCell(id);
				for (ImgEdge edge : vertex.getEdges()){
					Long destId = edge.getDestCellId();
					if (connectedGraph.contains(destId) && !checked.contains(destId) && checked.contains(vertex.getId())){
						checked.add(destId);
						modified = true;
					}
				}
				
			}
		}
		
		boolean result = true;
		for (Long id : connectedGraph){
			if (!checked.contains(id))
				result = false;
		}
		
		return result;
	}

	private static boolean allValuesFound(
			int joinElements,
			List<Tuple<Tuple<String, Object>, List<Tuple<Long, Integer>>>> tuplesList,
			List<Tuple<String, Object>> valueFound) {
		boolean allFound = true;
	
		for(int i = 0; i<joinElements; i++){
			String attribute = tuplesList.get(i).getX().getX();
			Object value = tuplesList.get(i).getX().getY() ;
			boolean present = false;
			for(Tuple<String,Object> tuple : valueFound){
				if (tuple.getX().equals(attribute) && tuple.getY().equals(value)){
					present = true;
				}
			}
			if (!present)
				allFound = false;
		}

		return allFound;
	}

	private static boolean checkIfValuePresentInNeighborhoodVector(int joinElements, List<Tuple<Tuple<String, Object>, List<Tuple<Long, Integer>>>> tuplesList, ImgVertex vertex) {
		boolean found = false;
		Map<String, List<Tuple<Object, Integer>>> vector = vertex.getNeighborhoodVector().getVector();
		for(int i = 0; i<joinElements; i++){
			String attribute = tuplesList.get(i).getX().getX();
			Object value = tuplesList.get(i).getX().getY();
			
			for (Entry<String, List<Tuple<Object, Integer>>> entry : vector.entrySet()){
				if (entry.getKey().equals(attribute)){
					for (Tuple<Object, Integer> tuple : entry.getValue()){
						if (tuple.getX().equals(value))
							found = true;
					}
				}
			}
			
		}
		return found;
	}	
	
	private static boolean checkIfNewValueFound(
			int joinElements,
			List<Tuple<Tuple<String, Object>, List<Tuple<Long, Integer>>>> tuplesList,
			List<Tuple<String, Object>> valueFound, ImgVertex vertex) {
		boolean found = false;
		for(int i = 0; i<joinElements; i++){
			String attribute = tuplesList.get(i).getX().getX();
			Object value = vertex.getAttribute(attribute);
			//This vertice contain this attribute
			if (value != null){
				//This vertice contain this value
				if (value.equals(tuplesList.get(i).getX().getY())){
					
					//This {attribute, value} has not been found yet.
					boolean exist = false;
					for(Tuple<String,Object> tuple : valueFound){
						if (!exist && tuple.getX().equals(attribute) && tuple.getY().equals(value)){
							exist = true;
						}
					}
					if (!exist){
						found = true;
						valueFound.add(new Tuple<String,Object>(attribute, value));
					}
				}
			}
			
		}
		return found;
	}
	
	public static boolean isNumeric(String inputData) {
		return inputData.matches("[0-9]+");
	}
}