package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ComputeMetaPathsStartingFromNodeIdThread implements Callable {
	private final int                                        start_nodeId;
	private final int                                        metaPathLength;
	private final HeavyGraph                                 graph;
	private final Log                                        log;
	private final float                                      edgeSkipProbability;
	private final int                                        AVERAGE_NODE_DEGREE = 3;
	private       Map<Integer, ArrayList<MultiTypeMetaPath>> foundMetaPaths;

	ComputeMetaPathsStartingFromNodeIdThread(int start_nodeId, int metaPathLength, HeavyGraph graph, Log log) {
		this.start_nodeId = start_nodeId;
		this.metaPathLength = metaPathLength;
		this.edgeSkipProbability = 0;
		this.graph = graph;
		this.log = log;
		this.foundMetaPaths = new HashMap<>((int) Math.pow(AVERAGE_NODE_DEGREE, metaPathLength));
	}

	public ComputeMetaPathsStartingFromNodeIdThread(int start_nodeId, int metaPathLength, float edgeSkipProbability, HeavyGraph graph, Log log) {
		this.start_nodeId = start_nodeId;
		this.metaPathLength = metaPathLength;
		this.edgeSkipProbability = edgeSkipProbability;
		this.graph = graph;
		this.log = log;
		this.foundMetaPaths = new HashMap<>((int) Math.pow(AVERAGE_NODE_DEGREE, metaPathLength));
	}

	public Map<Integer, ArrayList<MultiTypeMetaPath>> computeMetaPathFromNodeID(int start_nodeId, int metaPathLength) {
		MultiTypeMetaPath initialMetaPath = new MultiTypeMetaPath(metaPathLength);

		computeMetaPathFromNodeID(initialMetaPath, start_nodeId, metaPathLength - 1);
		if (!this.foundMetaPaths.keySet().isEmpty()) {
			log.info("Calculated meta-paths for " + start_nodeId);
		} else {
			log.info("Found no meta-paths for " + start_nodeId);
		}

		return this.foundMetaPaths;
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

	@Override public Map<Integer, ArrayList<MultiTypeMetaPath>> call() throws Exception {
		return computeMetaPathFromNodeID(start_nodeId, metaPathLength);
	}
}
