package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ComputeMetaPathsFromNodeIdSelectedThread extends ComputeMetaPathsStartingFromNodeIdThread {

	private Set<Integer> endNodes;

	ComputeMetaPathsFromNodeIdSelectedThread(int start_nodeId, int metaPathLength, HeavyGraph graph, Log log, ArrayList<Integer> endNodes) {
		super(start_nodeId, metaPathLength, graph, log);
		this.endNodes = new HashSet<Integer>(endNodes);
	}

	public ComputeMetaPathsFromNodeIdSelectedThread(int start_nodeId, int metaPathLength, float edgeSkipProbability, HeavyGraph graph, Log log, ArrayList<Integer> endNodes) {
		super(start_nodeId, metaPathLength, edgeSkipProbability, graph, log);
		this.endNodes = new HashSet<Integer>(endNodes);
	}

	void computeMetaPathFromNodeID(MultiTypeMetaPath currentMultiTypeMetaPath, int currentInstance, int metaPathLength) {
		if (metaPathLength <= 0)
			return;

		int[] labels = graph.getLabels(currentInstance);
		if (this.endNodes.contains(currentInstance)) {
			this.saveMetaPaths(currentMultiTypeMetaPath, currentInstance, labels);
		}

		this.recurse(currentMultiTypeMetaPath, currentInstance, metaPathLength, labels);

	}
}
