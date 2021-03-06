package com.tinkerpop.blueprints.impls.imgraph;



import org.jgroups.util.UUID;

import com.imgraph.common.ImgLogger;
import com.imgraph.loader.TextFileLoader;
import com.imgraph.networking.messages.LoadMessage.LoadFileType;
import com.imgraph.storage.ImgpFileTools;

/**
 * @author Aldemar Reynaga
 * Blueprints point of access for the batch loader
 */
public class ImgraphBatchLoader {

		
			
	public static void loadImgFile(String fileName) {
		ImgraphGraph graph = ImgraphGraph.getInstance();
		ImgpFileTools fileLoader =  new ImgpFileTools(UUID.randomUUID().toString());
		try {
			fileLoader.readFromFile(graph, fileName);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try { fileLoader.closeClientThreads();}catch(Exception x) {}
		}
	}
	
	public static void loadTextFile(String fileName, boolean directedEdges, boolean adjacentListFile) {
		TextFileLoader loader = new TextFileLoader();
		
		LoadFileType loadFileType = adjacentListFile?LoadFileType.ADJ_LIST_TEXT_FILE:
					LoadFileType.SIMPLE_TEXT_FILE;
		try {
			loader.load(fileName, loadFileType, directedEdges);
		} catch (Exception e) {
			ImgLogger.logError(e, "Error loading text file");
		} finally {
			loader.close();
		}
	}
	
}
