package com.imgraph.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.awt.Desktop;

import org.infinispan.Cache;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.imgraph.common.BigTextFile;
import com.imgraph.model.Cell;
import com.imgraph.model.EdgeType;
import com.imgraph.model.ImgEdge;
import com.imgraph.model.ImgVertex;
import com.imgraph.testing.TestTools;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.impls.imgraph.ImgraphGraph;
import com.tinkerpop.blueprints.impls.imgraph.ImgraphVertex;

/**
 * @author Aldemar Reynaga
 * Text file functions used for the loading of files
 */
public class FileUtilities {
	
	
	
	
	public static void readFromFile(ImgraphGraph graph, String fileName) throws Exception {
		BigTextFile file = null;
		StringTokenizer tokenizer = null;
		ImgVertex vertex = null;
		int inEdgesCounter, outEdgesCounter;
		long counter=0;
		Date startDate, endDate;
		try {
			startDate = new Date();
			file = new BigTextFile(fileName);
			
			System.out.print("Loading\n[");
			for (String line : file) {
				if (!line.trim().equals("")) {
					tokenizer = new StringTokenizer(line);
					vertex = ((ImgraphVertex)graph.addVertex(Long.parseLong(tokenizer.nextToken()))).getRawVertex();
					
					inEdgesCounter = Integer.parseInt(tokenizer.nextToken());
					for (int i=0; i<inEdgesCounter; i++) 
						vertex.addPartialEdge(Long.parseLong(tokenizer.nextToken()), EdgeType.IN, "");
					
					outEdgesCounter = Integer.parseInt(tokenizer.nextToken());
					for (int i=0; i<outEdgesCounter; i++) 
						vertex.addPartialEdge(Long.parseLong(tokenizer.nextToken()), EdgeType.OUT, "");
					
					counter++;
					
					if (counter%500==0) {
						System.out.print(".");
						System.out.flush();
						graph.stopTransaction(Conclusion.SUCCESS);
					} else if ((counter % 50000) == 0) {
						System.out.print("]\nLoaded " + counter + " lines \n[" );
						System.out.flush();
					}
					
				}
			}
			graph.stopTransaction(Conclusion.SUCCESS);
			
			
			System.out.println("Starting to update edgeAddresses...");
			EdgeAddressesUpdater.updateEdgeAddresses();
			
			
			
			System.out.println("Starting to calculate local 1-Hop neighbors");
			//ImgpFileTools.updateLocal1HopNeighbors();
			
			
			endDate =  new Date();


			System.out.println("File succesfully loaded in " + (endDate.getTime() - startDate.getTime()) + 
					"ms. "+ counter + " vertices have been processed");
			
			
		} finally {
			if (file != null) file.Close();
		}
		
	}
	
	
	public static void writeToFile(String fileName) throws IOException {
		Cache<Long, Cell> cache = CacheContainer.getCellCache();
		String line = null, edgeIn = null, edgeOut = null, edgeUnd = null;
		int edgeInCounter, edgeOutCounter, edgeUndCounter;
		BufferedWriter bufWriter = null;
		
		try {
			bufWriter = new BufferedWriter(new FileWriter(fileName));
			
			for (Cell cell : cache.values()) {
				
				
				if (cell instanceof ImgVertex) {
					line = cell.getId() + "\t";
					edgeIn = edgeOut = edgeUnd = "";
					edgeInCounter = edgeOutCounter = edgeUndCounter = 0;
					
					for (ImgEdge edge : ((ImgVertex) cell).getEdges()){
						
						switch (edge.getEdgeType()) {
						case HYPEREDGE:
							break;
						case IN:
							edgeIn += ("\t" + edge.getDestCellId());
							edgeInCounter++;
							break;
						case OUT:
							edgeOut += ("\t" + edge.getDestCellId());
							edgeOutCounter++;
							break;
						case UNDIRECTED:
							edgeUnd += ("\t" + edge.getDestCellId());
							edgeUndCounter++;
							break;
						default:
							break;
						
						}
					}
					
					line += (edgeInCounter + edgeIn + "\t" + edgeOutCounter + edgeOut +
							"\t" + edgeUndCounter + edgeUnd);
					
					bufWriter.write(line);
					bufWriter.newLine();
				}
			}
		} finally {
			if (bufWriter!=null){try{bufWriter.close();}catch(IOException ioe){}}
		}
	}
	
	public static void writeD3ToFile(String fileName, Long maxID) throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> mapObject = new HashMap<String, Object>();
	
		Map<Long,Map<Long,String>> connectionsMap = TestTools.getConnections(maxID);
		//First Loop : create all vertices using keySet()
		Map<Long, Integer> indexMap = new HashMap<Long, Integer>();
		List<Object> nodeList = new ArrayList<Object>();
		int i = 0;
		for (Long id : connectionsMap.keySet()){
			Map<String, Object> node = new HashMap<String, Object>();
			node.put("name", "Vertex " + id);
			node.put("group", StorageTools.getCellAddress(id));
			nodeList.add(node);
			indexMap.put(id, i);
			i++;
		}
		mapObject.put("nodes", nodeList);
		
		//Second Loop : create all edges
		List<Object> edgeList = new ArrayList<Object>();
		Iterator<Entry<Long, Map<Long, String>>> entries = connectionsMap.entrySet().iterator();
		while (entries.hasNext()) {
			Entry<Long, Map<Long, String>> vertex = entries.next();
			Long vertexId = vertex.getKey();
			Map<Long,String> edges = vertex.getValue(); //Edges
			for (Long edgeId : edges.keySet()){
				Map<String, Object> edge = new HashMap<String, Object>();
				edge.put("source", indexMap.get(vertexId));
				edge.put("target", indexMap.get(edgeId));
				edge.put("value", 1);
				edgeList.add(edge);
			}
		}
		mapObject.put("links", edgeList);
		
		try {
			objectMapper.writeValue(new File(fileName), mapObject);
			if(Desktop.isDesktopSupported())
			{
			  Desktop.getDesktop().browse(new File("../data/index.html").toURI());
			}
			//file:///Users/romain/Dropbox/Master/Q2/Memoire/Code/Steffi/imgraph-core/data/index.html

		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}