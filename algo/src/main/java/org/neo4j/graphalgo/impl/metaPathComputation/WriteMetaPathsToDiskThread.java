package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

public class WriteMetaPathsToDiskThread implements Runnable {
	private final int   AVERAGE_NODE_TYPES      = 5;
	private final float METAPATH_DUPLICATE_RATE = 0.8f;
	private final int        metaPathLength;
	private final Log        log;
	private final int        thread_startnode;
	private final HeavyGraph graph;
	private final float      edgeSkipProbability;

	Map<Integer, ArrayList<MultiTypeMetaPath>> results;

	public WriteMetaPathsToDiskThread(Map<Integer, ArrayList<MultiTypeMetaPath>> results, int metaPathLength, Log log, int thread_startnode, HeavyGraph graph,
			float edgeSkipProbability) {
		this.results = results;
		this.metaPathLength = metaPathLength;
		this.log = log;
		this.thread_startnode = thread_startnode;
		this.graph = graph;
		this.edgeSkipProbability = edgeSkipProbability;
	}

	@Override public void run() {
		for (Integer end_nodeID : this.results.keySet()) {
			ArrayList<MultiTypeMetaPath> multiTypeMetaPaths = this.results.get(end_nodeID);

			// Remove duplicates
			HashSet<String> metaPathStrings = new HashSet<>((int) (Math.pow(this.AVERAGE_NODE_TYPES, metaPathLength) * this.METAPATH_DUPLICATE_RATE) * multiTypeMetaPaths.size());
			for (MultiTypeMetaPath metaPath : multiTypeMetaPaths) {
				metaPathStrings.addAll(MultiTypeMetaPath.getStrings(MultiTypeMetaPath.composeMetaPaths(metaPath)));
			}

			new File("/tmp/between_instances").mkdirs();
			try {
				PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(
						"/tmp/between_instances/MetaPaths-" + metaPathLength + "-" + edgeSkipProbability + "_" + graph.toOriginalNodeId(thread_startnode) + "_" + graph
								.toOriginalNodeId(end_nodeID) + ".txt")));
				for (String metaPathString : metaPathStrings) {
					out.println(metaPathString);
				}
				out.flush();
				out.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				log.error("FileNotFoundException occured: " + e.toString());
			}
		}

		log.info("Wrote meta-paths for " + thread_startnode);
	}
}
