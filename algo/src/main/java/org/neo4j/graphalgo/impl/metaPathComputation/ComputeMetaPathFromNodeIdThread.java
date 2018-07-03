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
	private final int             start_nodeId;
	private final int             metaPathLength;
	private final HeavyGraph      graph;
	private final Log             log;
	private final float           edgeSkipProbability;
	private Map<Integer, ArrayList<ArrayList<Integer[]>>> foundMetaPaths;
	private final int AVERAGE_NODE_DEGREE = 3;

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
		ArrayList<Integer[]> initialMetaPath = new ArrayList<>();

		computeMetaPathFromNodeID(initialMetaPath, start_nodeId, metaPathLength - 1);
		log.info("Calculated meta-paths for " + start_nodeId);

		for (Integer end_nodeID : this.foundMetaPaths.keySet()) {
			ArrayList<ArrayList<Integer[]>> multiTypeMetaPaths = this.foundMetaPaths.get(end_nodeID);
			List<String> metaPaths = multiTypeMetaPaths.stream().map(this::returnMetaPaths).collect(ArrayList<String>::new, ArrayList::addAll, ArrayList::addAll);
			new File("/tmp/between_instances").mkdir();
			try {
				PrintStream out = new PrintStream(new FileOutputStream(
						"/tmp/between_instances/MetaPaths-" + metaPathLength + "-" + this.edgeSkipProbability + "_" + graph.toOriginalNodeId(start_nodeId) + "_" + graph
								.toOriginalNodeId(end_nodeID) + ".txt"));
				for (String mp : metaPaths) {
					out.println(mp);
				}
				out.flush();
				out.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				log.error("FileNotFoundException occured: " + e.toString());
			}
		}
	}

	private void computeMetaPathFromNodeID(ArrayList<Integer[]> currentMultiTypeMetaPath, int currentInstance, int metaPathLength) {
		if (metaPathLength <= 0)
			return;

		Integer[] labels = graph.getLabels(currentInstance);
		if (currentMultiTypeMetaPath.size() > 0) {
			ArrayList<Integer[]> newMultiTypeMetaPath = new ArrayList<>(currentMultiTypeMetaPath);
			newMultiTypeMetaPath.add(labels);

			ArrayList<ArrayList<Integer[]>> previouslyFoundMetaPathsForID = this.foundMetaPaths.get(currentInstance);
			if (previouslyFoundMetaPathsForID != null) {
				previouslyFoundMetaPathsForID.add(newMultiTypeMetaPath);
			} else {
				ArrayList<ArrayList<Integer[]>> list = new ArrayList<>();
				list.add(newMultiTypeMetaPath);
				this.foundMetaPaths.put(currentInstance, list);
			}
		}

		IntStream.of(graph.getAdjacentNodes(currentInstance)).filter(x -> ThreadLocalRandom.current().nextFloat() > this.edgeSkipProbability).mapToObj(node -> {
			ArrayList<Integer[]> newMultiTypeMetaPath = new ArrayList<>(currentMultiTypeMetaPath);
			newMultiTypeMetaPath.add(labels);
			newMultiTypeMetaPath.add(new Integer[] { graph.getEdgeLabel(currentInstance, node) });
			computeMetaPathFromNodeID(newMultiTypeMetaPath, node, metaPathLength - 1);
			return null;
		}).collect(Collectors.toList());
	}

	public List<String> returnMetaPaths(ArrayList<Integer[]> metaPathParts) {
		Set<List<Integer>> allMetaPaths = composeMetaPaths(metaPathParts);
		return stringifyMetaPaths(allMetaPaths);
	}

	public List<String> stringifyMetaPaths(Set<List<Integer>> allMetaPaths) {
		return allMetaPaths.stream().map(list -> list.stream().map(Object::toString).collect(Collectors.joining("|"))).collect(Collectors.toList());
	}

	public Set<List<Integer>> composeMetaPaths(ArrayList<Integer[]> metaPathParts) {
		List<Set<Integer>> interimList = metaPathParts.stream().map(list -> new HashSet<>(Arrays.asList(list))).collect(Collectors.toList());
		return Sets.cartesianProduct(interimList);
	}

	public void run() {
		computeMetaPathFromNodeID(start_nodeId, metaPathLength);
	}
}
