package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ComputeAllMetaPathsStartingAtInstances extends MetaPathComputation {

	private final int        startNodeID;
	private final int        endNodeID;
	public        Log        log;
	private       int        metaPathLength;
	private       HeavyGraph graph;
	private       float      nodeSkipProbability = 0;
	private       float      edgeSkipProbability = 0;

	public ComputeAllMetaPathsStartingAtInstances(HeavyGraph graph, int metaPathLength, Log log) {
		this.metaPathLength = metaPathLength;
		this.graph = graph;
		this.log = log;
		this.startNodeID = Integer.MIN_VALUE;
		this.endNodeID = Integer.MAX_VALUE;
	}

	public ComputeAllMetaPathsStartingAtInstances(HeavyGraph graph, int metaPathLength, Log log, float nodeSkipProbability, float edgeSkipProbability, int startNodeID,
			int endNodeID) {
		this.metaPathLength = metaPathLength;
		this.graph = graph;
		this.log = log;
		this.nodeSkipProbability = nodeSkipProbability;
		this.edgeSkipProbability = edgeSkipProbability;
		this.startNodeID = startNodeID;
		this.endNodeID = endNodeID;
	}

	public Result compute() {
		log.info("START BETWEEN_INSTANCES");

		long startTime = System.nanoTime();
		startThreads();
		long endTime = System.nanoTime();
		log.info("FINISH BETWEEN_INSTANCES after " + (endTime - startTime) / 1000000 + " milliseconds");

		return new Result(new HashSet<>());
	}

	private void startThreads() {
		int processorCount = Runtime.getRuntime().availableProcessors();
		log.info("ProcessorCount: " + processorCount);
		ExecutorService executor = Executors.newFixedThreadPool(processorCount);

		Random random = new Random(42);

		List<Future<?>> futures = new ArrayList<>();
		Map<Future<?>, Integer> thread_startnode = new HashMap<>();
		graph.forEachNode(node -> {
			//TODO: Remove hardcoded "Entity" with id 22
			if (this.startNodeID <= node && node < endNodeID && node != 22 && random.nextFloat() > this.nodeSkipProbability) {
				Future<?> future = executor.submit(new ComputeMetaPathsStartingFromNodeIdAllThread(node, metaPathLength, this.edgeSkipProbability, graph, log));
				futures.add(future);
				thread_startnode.put(future, node);
			}
			return true;
		});
		executor.shutdown();

		ExecutorService writeExecutor = Executors.newFixedThreadPool(processorCount);
		while (!futures.isEmpty()) {
			if (executor.isTerminated()) {
				log.info("Calculation of meta-paths finished, still writing results on disk...");
			}
			Iterator<Future<?>> iterator = futures.iterator();
			while (iterator.hasNext()) {
				Future<?> next = iterator.next();
				if (next.isDone()) {
					try {
						Map<Integer, ArrayList<MultiTypeMetaPath>> results = (Map<Integer, ArrayList<MultiTypeMetaPath>>) next.get();
						if (!results.isEmpty()) {
							writeExecutor.execute(new WriteMetaPathsToDiskThread(results, metaPathLength, log, thread_startnode.get(next), graph, edgeSkipProbability));
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}

					iterator.remove();
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		writeExecutor.shutdown();
		while (!writeExecutor.isTerminated()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	//TODO -------------------------------------------------------------------

	@Override public ComputeAllMetaPathsStartingAtInstances me() {
		return this;
	}

	@Override public ComputeAllMetaPathsStartingAtInstances release() {
		return null;
	}

	/**
	 * Result class used for streaming
	 */
	public static final class Result {

		HashSet<String> finalMetaPaths;

		public Result(HashSet<String> finalMetaPaths) {
			this.finalMetaPaths = finalMetaPaths;
		}

		@Override public String toString() {
			return "Result{}";
		}

		public HashSet<String> getFinalMetaPaths() {
			return finalMetaPaths;
		}
	}
}