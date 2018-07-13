package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ComputeAllMetaPathsStartingAtInstancesWholeGraph extends ComputeAllMetaPathsStartingAtInstances {

	public ComputeAllMetaPathsStartingAtInstancesWholeGraph(HeavyGraph graph, int metaPathLength, Log log) {
		super(graph, metaPathLength, log);
	}

	public ComputeAllMetaPathsStartingAtInstancesWholeGraph(HeavyGraph graph, int metaPathLength, Log log, float nodeSkipProbability, float edgeSkipProbability, int startNodeID,
			int endNodeID) {
		super(graph, metaPathLength, log, nodeSkipProbability, edgeSkipProbability, startNodeID, endNodeID);
	}

	@Override protected void submitThreads(ExecutorService executor, List<Future<?>> futures, Map<Future<?>, Integer> thread_startnode) {
		Random random = new Random(42);
		graph.forEachNode(node -> {
			//TODO: Remove hardcoded "Entity" with id 22
			if (this.startNodeID <= node && node < endNodeID && node != 22 && random.nextFloat() > this.nodeSkipProbability) {
				Future<?> future = executor.submit(new ComputeMetaPathsStartingFromNodeIdAllThread(node, metaPathLength, this.edgeSkipProbability, graph, log));
				futures.add(future);
				thread_startnode.put(future, node);
			}
			return true;
		});
	}

}