package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ComputeMetaPathFromNodeIdThread implements Runnable {
	private final int        start_nodeId;
	private final int        metaPathLength;
	private final HeavyGraph graph;
	private final Log        log;
	private final float      edgeSkipProbability;
	private final int   AVERAGE_NODE_DEGREE     = 3;
	private final int   AVERAGE_NODE_TYPES      = 5;
	private final float METAPATH_DUPLICATE_RATE = 0.8f;
	private Map<Integer, ArrayList<MultiTypeMetaPath>> foundMetaPaths;

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
		if (!this.foundMetaPaths.keySet().isEmpty()) {
			log.info("Calculated meta-paths for " + start_nodeId);
		} else {
			log.info("Found no meta-paths for " + start_nodeId);
		}

		for (Integer end_nodeID : this.foundMetaPaths.keySet()) {
			ArrayList<MultiTypeMetaPath> multiTypeMetaPaths = this.foundMetaPaths.get(end_nodeID);
			new File("/tmp/between_instances").mkdirs();
			try {
				PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(
						"/tmp/between_instances/MetaPaths-" + metaPathLength + "-" + this.edgeSkipProbability + "_" + graph.toOriginalNodeId(start_nodeId) + "_" + graph
								.toOriginalNodeId(end_nodeID) + ".txt")));
				HashSet<String> metaPathStrings = new HashSet<>(
						(int) (Math.pow(this.AVERAGE_NODE_TYPES, metaPathLength) * this.METAPATH_DUPLICATE_RATE) * multiTypeMetaPaths.size());
				for (MultiTypeMetaPath metaPath : multiTypeMetaPaths) {
					metaPathStrings.addAll(MultiTypeMetaPath.getStrings(MultiTypeMetaPath.composeMetaPaths(metaPath)));
				}
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
