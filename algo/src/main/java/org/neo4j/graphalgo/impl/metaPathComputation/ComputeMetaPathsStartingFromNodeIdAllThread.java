package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

public class ComputeMetaPathsStartingFromNodeIdAllThread extends ComputeMetaPathsStartingFromNodeIdThread {

	ComputeMetaPathsStartingFromNodeIdAllThread(int start_nodeId, int metaPathLength, HeavyGraph graph, Log log) {
		super(start_nodeId, metaPathLength, graph, log);
	}

	public ComputeMetaPathsStartingFromNodeIdAllThread(int start_nodeId, int metaPathLength, float edgeSkipProbability, HeavyGraph graph, Log log) {
		super(start_nodeId, metaPathLength, edgeSkipProbability, graph, log);
	}

	void computeMetaPathFromNodeID(MultiTypeMetaPath currentMultiTypeMetaPath, int currentInstance, int metaPathLength) {
		if (metaPathLength <= 0)
			return;

		int[] labels = graph.getLabels(currentInstance);
		this.saveMetaPaths(currentMultiTypeMetaPath, currentInstance, labels);

		this.recurse(currentMultiTypeMetaPath, currentInstance, metaPathLength, labels);
	}
}
