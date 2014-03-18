package com.imgraph;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.Channel;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.stack.IpAddress;
import org.zeromq.ZMQ;

import com.imgraph.common.Configuration;
import com.imgraph.common.IOUtils;
import com.imgraph.common.ImgLogger;
import com.imgraph.index.ImgIndex;
import com.imgraph.index.ImgIndexHits;
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
import com.imgraph.networking.messages.LoadMessage.LoadFileType;
import com.imgraph.networking.messages.Message;
import com.imgraph.storage.CacheContainer;
import com.imgraph.storage.FileUtilities;
import com.imgraph.storage.ImgpFileTools;
import com.imgraph.storage.Local2HopNeighbors;
import com.imgraph.storage.StorageTools;
import com.imgraph.testing.Pair;
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

/**
 * @author Aldemar Reynaga
 * Creates a basic text console conceived for the execution of tests on basic functionalities, 
 * this console can be started using the parameter START_CONSOLE from the main JAR file 
 */
@SuppressWarnings("deprecation")
public class BasicConsole {
	
	public static void runConsoleNoZeroMQ() throws Exception {
		runConsole(false);
	}
	
	public static void runConsole() throws Exception {
		runConsole(true);
	}
	
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
				String connectedTo = "";
				String neighboursList = "";
				Iterator<Entry<Long, Map<Long, String>>> entries = connectionsMap.entrySet().iterator();
				while (entries.hasNext()) {
					result = "";
					Entry<Long, Map<Long, String>> vertexInfo = entries.next();
					Long vertexID = vertexInfo.getKey();
					Map<Long,String> edges = vertexInfo.getValue(); //Edges
					
					result += "Vertex " + vertexID + " : "										//VertexID
							+ "\n\t - is stored @ " + StorageTools.getCellAddress(vertexID);	//Address
					if (edges.size()>0){
						result += "\n\t - is connected to [";
						connectedTo = "";
						for(Map.Entry<Long,String> edge : edges.entrySet()) {					//Connected To
							if(connectedTo.equals(""))
								connectedTo += edge.getKey();
							else
								connectedTo += ","+edge.getKey();
						}
						connectedTo += "]";
					}
					System.out.println(result+connectedTo);	
														
					ImgVertex vertex = (ImgVertex) graph.getRawGraph().retrieveCell(vertexID);	//Attributes
					for (String key : vertex.getAttributeKeys()){
						System.out.println("\t - " + key + " : " + vertex.getAttribute(key));
					}
					
					Map<Pair<Object>, Float> neighbours = TestTools.getNeighborhoodVector(vertexID);		//NeighbourVector
					neighboursList = "\t - neighbour vector [";
					for (Entry<Pair<Object>, Float> entry : neighbours.entrySet()){
						neighboursList += "{"+entry.getKey()+","+entry.getValue()+"},";
					}
					neighboursList += "]"; //TODO substring to delete the last ,
					System.out.println(neighboursList);	
				}
				//Print Machines
				int i = 1;
				for (Address address : CacheContainer.getCacheContainer().getTransport().getMembers()){
					System.out.println("Machine "+i+"'s Address : "+ address.toString()+", IpAddress : "+StorageTools.getIpAddress(address));
					i++;
				}
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
					System.out.println("Vertice "+entry.getKey()+", stored @ "+StorageTools.getCellAddress(entry.getKey())+" is connected to :");
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
					long numVertices = Long.parseLong(IOUtils.readLine("Number of vertices: "));
					long minId = Long.parseLong(IOUtils.readLine("Minimum id: "));
					long maxId = Long.parseLong(IOUtils.readLine("Maximum id: "));
					TestTools.genVertices(minId, maxId, numVertices);

					Map<String, Integer> cellCount = StorageTools.countCellsInCluster();
					long totalCount = 0;
					for (Entry<String, Integer> entry : cellCount.entrySet()) {
						System.out.println("Machine " + entry.getKey() + ": " + entry.getValue() + " cells");
						totalCount += entry.getValue();
					}	
					System.out.println("Total Count = " + totalCount);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (command.equals("genEdges")) {
				try {	
					int numEdges = Integer.parseInt(IOUtils.readLine("Number of edges: "));
					long minId = Long.parseLong(IOUtils.readLine("Minimum start vertex id: "));
					long maxId = Long.parseLong(IOUtils.readLine("Maximum end vertex id: "));
					boolean directed = IOUtils.readLine("Directed (Y/N): ").equals("Y");
					TestTools.genEdges(minId, maxId, numEdges, directed);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (command.equals("getAV")) {//TODO getAddressesVertex
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
			} else if (command.equals("move")) { //TODO move
				try {			
					//Choose which node to move
					long id = Long.parseLong(IOUtils.readLine("Vertex Id : "));	
					//Print Machines
					Map<String, Integer> indexMap = graph.getRawGraph().getMemberIndexes();
					for(Entry<String, Integer> entry : indexMap.entrySet()){
						System.out.println("Machine "+entry.getValue()+"'s Address : "+ entry.getKey());
					}
					//Choose on which machine to move
					int index = Integer.parseInt(IOUtils.readLine("Machine index : "));
					String machineName = "";
					for (Entry<String, Integer> entry : indexMap.entrySet()){
						if (entry.getValue() == index)
							machineName=entry.getKey();
					}
					System.out.println("Vertex "+id+" will be moved to "+ machineName);
					
					if(ImgraphGraph.getInstance().getRawGraph().retrieveCell(id) != null
							&& !machineName.equals("")){
					
						//Save edges
						List<ImgEdge> savedEdges = ((ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(id)).getEdges();
						
						//Remove the cell
						graph.startTransaction();
						((ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(id)).remove();
						graph.stopTransaction(Conclusion.SUCCESS);
						
						//Find a free id on the target machine
						long newId = 0;
						List<Long> vertexIds = new ArrayList<Long>();
						for (long l = 0; l < id*100; l++){//TODO increase id if no free id found
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
							TestTools.genVertice(newId, savedEdges);
							
							socket.close();
						}finally {
							if (socket !=null)
								socket.close();
						}
					}
					else 
						System.out.println("Bad input");
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (command.equals("remove")) {
				command  = IOUtils.readLine("Cell ID: ");
				
				graph.startTransaction();
				((ImgVertex) ImgraphGraph.getInstance().getRawGraph().retrieveCell(Long.parseLong(command))).remove();
				graph.commit();
			} else if (command.equals("getLVI")) { //TODO LocalVertexIds
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
				CacheContainer.getCellCache().clear();
				System.out.println("Cell cache cleared");
			} else if (command.equals("exit")) {
				System.out.println("BYE...");
				Main.sendStopMessage(Configuration.getProperty(Configuration.Key.NODE_PORT));
			
				break;
			}			
		}
	}	
}