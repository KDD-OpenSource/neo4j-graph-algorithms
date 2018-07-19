package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ComputeAllMetaPathsStartingAtInstancesEdgeList extends ComputeAllMetaPathsStartingAtInstances {
	private Map<Integer, ArrayList<Integer>> edges;

	public ComputeAllMetaPathsStartingAtInstancesEdgeList(HeavyGraph graph, int metaPathLength, Log log, ArrayList<Integer[]> edgelist) {
		super(graph, metaPathLength, log);
		this.edges = new HashMap<>();
		addEdgesToMap(edgelist);
	}

	public ComputeAllMetaPathsStartingAtInstancesEdgeList(HeavyGraph graph, int metaPathLength, Log log, float nodeSkipProbability, float edgeSkipProbability, int startNodeID,
			int endNodeID, ArrayList<Integer[]> edgelist) {
		super(graph, metaPathLength, log, nodeSkipProbability, edgeSkipProbability, startNodeID, endNodeID);
		this.edges = new HashMap<>();
		addEdgesToMap(edgelist);
	}

	private void addEdgesToMap(ArrayList<Integer[]> edgelist) {
		log.info(edgelist.size() + " edges in the edgelist");
		for (Integer[] edge : edgelist) {
			ArrayList<Integer> list_edge1 = this.edges.get(edge[1]);
			if (list_edge1 == null || !list_edge1.contains(edge[0])) {
				ArrayList<Integer> previouslyAddedEndNodes = this.edges.get(edge[0]);
				if (previouslyAddedEndNodes != null) {
					if (!previouslyAddedEndNodes.contains(edge[1])) {
						previouslyAddedEndNodes.add(edge[1]);
					}
				} else {
					ArrayList<Integer> list = new ArrayList<>();
					list.add(edge[1]);
					this.edges.put(edge[0], list);
				}
			}
		}
		log.info("Have to mine meta-paths for " + this.edges.size() + " nodes");
	}

	@Override protected void submitThreads(ExecutorService executor, List<Future<?>> futures, Map<Future<?>, Integer> thread_startnode) {
		Random random = new Random(42);
		for (Integer startNode : edges.keySet()) {
			//TODO: Remove hardcoded "Entity" with id 22
			if (this.startNodeID <= startNode && startNode < endNodeID && startNode != 22 && random.nextFloat() > this.nodeSkipProbability) {
				Future<?> future = executor
						.submit(new ComputeMetaPathsFromNodeIdSelectedThread(startNode, metaPathLength, this.edgeSkipProbability, graph, log, this.edges.get(startNode)));
				futures.add(future);
				thread_startnode.put(future, startNode);
			}
		}
	}
}