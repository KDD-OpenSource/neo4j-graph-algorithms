package org.neo4j.graphalgo.impl.metaPathComputation;

import com.google.common.collect.Sets;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ComputeMetaPathFromNodeIdThread implements Runnable {
	private final int                                        start_nodeId;
	private final int                                        metaPathLength;
	private final HeavyGraph                                 graph;
	private final Log                                        log;
	private final float                                      edgeSkipProbability;
	private final int AVERAGE_NODE_DEGREE = 3;
	private       Map<Integer, ArrayList<MultiTypeMetaPath>> foundMetaPaths;

	ComputeMetaPathFromNodeIdThread(int start_nodeId, int metaPathLength, HeavyGraph graph, Log log) {
		this.start_nodeId = start_nodeId;
		this.metaPathLength = metaPathLength;
		this.edgeSkipProbability = 0;
		this.graph = graph;
		this.log = log;
		this.foundMetaPaths = new HashMap<>((int) Math.pow(AVERAGE_NODE_DEGREE, metaPathLength));
	}

	public ComputeMetaPathFromNodeIdThread(int start_nodeId, int metaPathLength, float edgeSkipProbability, HeavyGraph graph, Log log) {
		this.start_nodeId = start_nodeId;
		this.metaPathLength = metaPathLength;
		this.edgeSkipProbability = edgeSkipProbability;
		this.graph = graph;
		this.log = log;
		this.foundMetaPaths = new HashMap<>((int) Math.pow(AVERAGE_NODE_DEGREE, metaPathLength));
	}

	public void computeMetaPathFromNodeID(int start_nodeId, int metaPathLength) {
		MultiTypeMetaPath initialMetaPath = new MultiTypeMetaPath(metaPathLength);

		computeMetaPathFromNodeID(initialMetaPath, start_nodeId, metaPathLength - 1);
		log.info("Calculated meta-paths for " + start_nodeId);

		for (Integer end_nodeID : this.foundMetaPaths.keySet()) {
			ArrayList<MultiTypeMetaPath> multiTypeMetaPaths = this.foundMetaPaths.get(end_nodeID);
			new File("/tmp/between_instances").mkdir();
			try {
				PrintStream out = new PrintStream(new FileOutputStream(
						"/tmp/between_instances/MetaPaths-" + metaPathLength + "-" + this.edgeSkipProbability + "_" + graph.toOriginalNodeId(start_nodeId) + "_" + graph
								.toOriginalNodeId(end_nodeID) + ".txt"));
				for(MultiTypeMetaPath metaPath:multiTypeMetaPaths) {
					out.println(metaPath.toString());
				}
				out.flush();
				out.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				log.error("FileNotFoundException occured: " + e.toString());
			}
		}
	}

	private void computeMetaPathFromNodeID(MultiTypeMetaPath currentMultiTypeMetaPath, int currentInstance, int metaPathLength) {
		if (metaPathLength <= 0)
			return;

		int[] labels = graph.getLabels(currentInstance);
		if (currentMultiTypeMetaPath.length() > 0) {
			MultiTypeMetaPath newMultiTypeMetaPath = new MultiTypeMetaPath(currentMultiTypeMetaPath);
			newMultiTypeMetaPath.addLastNodeLabels(labels);

			ArrayList<MultiTypeMetaPath> previouslyFoundMetaPathsForID = this.foundMetaPaths.get(currentInstance);
			if (previouslyFoundMetaPathsForID != null) {
				previouslyFoundMetaPathsForID.add(newMultiTypeMetaPath);
			} else {
				ArrayList<MultiTypeMetaPath> list = new ArrayList<>();
				list.add(newMultiTypeMetaPath);
				this.foundMetaPaths.put(currentInstance, list);
			}
		}

		IntStream.of(graph.getAdjacentNodes(currentInstance)).filter(x -> ThreadLocalRandom.current().nextFloat() > this.edgeSkipProbability).mapToObj(node -> {
			MultiTypeMetaPath newMultiTypeMetaPath = new MultiTypeMetaPath(currentMultiTypeMetaPath);
			newMultiTypeMetaPath.add(labels, graph.getEdgeLabel(currentInstance, node));
			computeMetaPathFromNodeID(newMultiTypeMetaPath, node, metaPathLength - 1);
			return null;
		}).collect(Collectors.toList());
	}

	public void run() {
		computeMetaPathFromNodeID(start_nodeId, metaPathLength);
	}
}
