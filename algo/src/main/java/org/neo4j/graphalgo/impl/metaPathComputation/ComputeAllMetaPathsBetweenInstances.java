package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ComputeAllMetaPathsBetweenInstances extends MetaPathComputation {

    private int         metaPathLength;
    private HeavyGraph  graph;
    public  Log         log;
    private float nodePairSkipProbability = 0;
    private float edgeSkipProbability = 0;

    public ComputeAllMetaPathsBetweenInstances(HeavyGraph graph, int metaPathLength,  Log log){
        this.metaPathLength = metaPathLength;
        this.graph = graph;
        this.log = log;
    }

    public ComputeAllMetaPathsBetweenInstances(HeavyGraph graph, int metaPathLength,  Log log, float nodePairSkipProbability, float edgeSkipProbability){
        this.metaPathLength = metaPathLength;
        this.graph = graph;
        this.log = log;
        this.nodePairSkipProbability = nodePairSkipProbability;
        this.edgeSkipProbability = edgeSkipProbability;
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

        graph.forEachNode(node -> {
            int[] adjacent_nodes = graph.getAdjacentNodes(node);
            for(int adjacent_node: adjacent_nodes){
                if (random.nextFloat() > this.nodePairSkipProbability) {
                    Runnable worker = new ComputeMetaPathFromNodeIdThread(node, adjacent_node, metaPathLength, this.edgeSkipProbability);
                    executor.execute(worker);
                }
            }
            return true;
        });
        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private ArrayList<Integer> copyMetaPath(ArrayList<Integer> currentMetaPath) {
        return new ArrayList<>(currentMetaPath);
    }

    private class ComputeMetaPathFromNodeIdThread implements Runnable {
        private final int start_nodeId;
        private final int end_nodeID;
        private final int metaPathLength;
        private HashSet<String> duplicateFreeMetaPathsOfThread;
        private final float edgeSkipProbability;
        private final Random random = new Random(42);

        ComputeMetaPathFromNodeIdThread(int start_nodeId, int end_nodeID, int metaPathLength) {
            this.start_nodeId = start_nodeId;
            this.end_nodeID = end_nodeID;
            this.metaPathLength = metaPathLength;
            this.duplicateFreeMetaPathsOfThread = new HashSet<>();
            this.edgeSkipProbability = 0;
        }

        ComputeMetaPathFromNodeIdThread(int start_nodeId, int end_nodeID, int metaPathLength, float edgeSkipProbability) {
            this.start_nodeId = start_nodeId;
            this.end_nodeID = end_nodeID;
            this.metaPathLength = metaPathLength;
            this.duplicateFreeMetaPathsOfThread = new HashSet<>();
            this.edgeSkipProbability = edgeSkipProbability;
        }

        public void computeMetaPathFromNodeID(int start_nodeId, int end_nodeID, int metaPathLength) {
            ArrayList<Integer> initialMetaPath = new ArrayList<>();

            computeMetaPathFromNodeID(initialMetaPath, start_nodeId, end_nodeID, metaPathLength - 1);
            log.info("Calculated meta-paths between " + start_nodeId + " and " + end_nodeID + " save in " + new File("/tmp/between_instances").getAbsolutePath());
            if (!new File("/tmp/between_instances").exists()) {
                new File("/tmp/between_instances").mkdir();
            }
            try {
                PrintStream out = new PrintStream(new FileOutputStream(
                        "/tmp/between_instances/MetaPaths-" + metaPathLength + "-" + this.edgeSkipProbability + "_" + graph.toOriginalNodeId(start_nodeId) + "_" + graph
                                .toOriginalNodeId(end_nodeID) + ".txt"));
                for (String mp : duplicateFreeMetaPathsOfThread) {
                    out.println(mp);
                }
                out.flush();
                out.close();
                duplicateFreeMetaPathsOfThread = null;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                log.error("FileNotFoundException occured: " + e.toString());
            }
        }

        private void computeMetaPathFromNodeID(ArrayList<Integer> currentMetaPath, int currentInstance, int end_nodeID, int metaPathLength) {
            if (metaPathLength <= 0) return;

            if (currentInstance == end_nodeID) {
                ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                Integer[] labels = graph.getLabels(currentInstance);
                newMetaPath.add(random.nextInt(labels.length));
                addAndLogMetaPath(newMetaPath);
                return;
            }

            Integer[] labels = graph.getLabels(currentInstance);
            for(int node: graph.getAdjacentNodes(currentInstance)) {
                if (random.nextFloat() > this.edgeSkipProbability) {
                    ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                    newMetaPath.add(random.nextInt(labels.length));
                    newMetaPath.add(graph.getEdgeLabel(currentInstance, node));
                    computeMetaPathFromNodeID(newMetaPath, node, end_nodeID, metaPathLength - 1);
                }
            }
        }

        private void addAndLogMetaPath(ArrayList<Integer> newMetaPath) {
            String joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining("|"));
            duplicateFreeMetaPathsOfThread.add(joinedMetaPath);
        }

        public void run() {
            computeMetaPathFromNodeID(start_nodeId, end_nodeID, metaPathLength);
        }
    }

    //TODO -------------------------------------------------------------------

    @Override
    public ComputeAllMetaPathsBetweenInstances me() {
        return this;
    }

    @Override
    public ComputeAllMetaPathsBetweenInstances release() {
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

        @Override
        public String toString() {
            return "Result{}";
        }

        public HashSet<String> getFinalMetaPaths() {
            return finalMetaPaths;
        }
    }
}