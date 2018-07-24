package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ComputeMetaPathsFromNodeIdSelectedThread extends ComputeMetaPathsStartingFromNodeIdThread {

	private Set<Integer> endNodes;
	private final int limit;

	ComputeMetaPathsFromNodeIdSelectedThread(int start_nodeId, int metaPathLength, HeavyGraph graph, Log log, ArrayList<Integer> endNodes, int limit) {
		super(start_nodeId, metaPathLength, graph, log);
		this.endNodes = new HashSet<Integer>(endNodes);
		this.limit = limit;
	}

	public ComputeMetaPathsFromNodeIdSelectedThread(int start_nodeId, int metaPathLength, float edgeSkipProbability, HeavyGraph graph, Log log, ArrayList<Integer> endNodes, int limit) {
		super(start_nodeId, metaPathLength, edgeSkipProbability, graph, log);
		this.endNodes = new HashSet<Integer>(endNodes);
		this.limit = limit;
	}

	ComputeMetaPathsFromNodeIdSelectedThread(int start_nodeId, int metaPathLength, HeavyGraph graph, Log log, ArrayList<Integer> endNodes) {
		super(start_nodeId, metaPathLength, graph, log);
		this.endNodes = new HashSet<Integer>(endNodes);
		this.limit = -1;
	}

	public ComputeMetaPathsFromNodeIdSelectedThread(int start_nodeId, int metaPathLength, float edgeSkipProbability, HeavyGraph graph, Log log, ArrayList<Integer> endNodes) {
		super(start_nodeId, metaPathLength, edgeSkipProbability, graph, log);
		this.endNodes = new HashSet<Integer>(endNodes);
		this.limit = -1;
	}

	void computeMetaPathFromNodeID(MultiTypeMetaPath currentMultiTypeMetaPath, int currentInstance, int metaPathLength) {
		if (metaPathLength <= 0)
			return;

		int[] labels = graph.getLabels(currentInstance);
		if (this.endNodes.contains(currentInstance)) {
			this.saveMetaPaths(currentMultiTypeMetaPath, currentInstance, labels);
		}
		boolean recurse = false;
		if(this.limit > -1) {
			for (int endNode : endNodes) {
				ArrayList<MultiTypeMetaPath> foundMetaPathsForEndNode = this.foundMetaPaths.get(endNode);
				if (foundMetaPathsForEndNode != null) {
					if (foundMetaPathsForEndNode.size() < this.limit) {
						recurse = true;
					}
				} else {
					recurse = true;
				}
			}
		} else {
			recurse = true;
		}

		if(recurse) {
			this.recurse(currentMultiTypeMetaPath, currentInstance, metaPathLength, labels);
		}
	}
}
