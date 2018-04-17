package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.*;

import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Math.toIntExact;

public class ComputeAllMetaPathsWoExistenceCheck extends MetaPathComputation {

    private int metaPathLength;
    private PrintStream debugOut;
    public GraphDatabaseAPI api;
    private HashMap<Integer, HashSet<AbstractMap.SimpleEntry<Integer,Integer>>> adjacentNodesDict = new HashMap<>();
    //private HashMap<Integer, Label> nodeIDLabelsDict = new HashMap<Integer, Label>();
    private List<Node> nodes = null;
    private List<Relationship> rels = null;
    private HashSet<String> duplicateFreeMetaPaths = new HashSet<>();
    private PrintStream out;
    int printCount = 0;
    double estimatedCount;
    private long startTime;

    public ComputeAllMetaPathsWoExistenceCheck(int metaPathLength, GraphDatabaseAPI api) throws Exception {
        this.metaPathLength = metaPathLength;
        this.api = api;
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Schema_Debug.txt"));
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Schema.txt"));//ends up in root/tests //or in dockerhome
    }

    public Result compute() {
        debugOut.println("START");
        startTime = System.nanoTime();
        getMetaGraph();
        estimatedCount = Math.pow(nodes.size(), metaPathLength + 1);
        initializeDictionaries();
        ArrayList<ComputeMetaPathFromNodeLabelThread> threads = new ArrayList<>();
        int i = 0;
        for (Node node : nodes) {
            ComputeMetaPathFromNodeLabelThread thread = new ComputeMetaPathFromNodeLabelThread(this, "thread-" + i, toIntExact(node.getId()), metaPathLength);
            thread.start();
            threads.add(thread);
            i++;
        }
        for (ComputeMetaPathFromNodeLabelThread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return new Result(duplicateFreeMetaPaths);
    }

    private void getMetaGraph(){
        org.neo4j.graphdb.Result result = null;
        try (Transaction tx = api.beginTx()) {
            result = api.execute("CALL apoc.meta.graph()");
            tx.success();
        }
        Map<String, Object> row = result.next();
        nodes = (List<Node>) row.get("nodes");
        rels = (List<Relationship>) row.get("relationships");
    }

    private void initializeDictionaries(){
        for (Node node : nodes) {
            int nodeID = toIntExact(node.getId());

            HashSet<AbstractMap.SimpleEntry<Integer, Integer>> adjNodesSet = new HashSet<>();
            adjacentNodesDict.putIfAbsent(toIntExact(nodeID), adjNodesSet);
            for (Relationship rel : rels) {
                debugOut.println(rel);
                debugOut.println(rel.getAllProperties());
                try {
                    int adjNodeID = toIntExact(rel.getOtherNodeId(node.getId()));
                    int adjEdgeID = toIntExact(rel.getId());
                    adjacentNodesDict.get(nodeID).add(new AbstractMap.SimpleEntry<>(adjNodeID, adjEdgeID));
                } catch (Exception e) {/*prevent duplicates*/}
            }
        }
        out.println(adjacentNodesDict);
    }

    public void computeMetaPathFromNodeLabel(int nodeID, int metaPathLength) { //TODO will it be faster if not node but nodeID with dicts?
        ArrayList<Integer> initialMetaPath = new ArrayList<>();
        initialMetaPath.add(nodeID); //because node is already type (of nodes in the real graph)
        computeMetaPathFromNodeLabel(initialMetaPath, nodeID, metaPathLength - 1);
    }

    private void computeMetaPathFromNodeLabel(ArrayList<Integer> pCurrentMetaPath, int pCurrentInstance, int pMetaPathLength) {
        Stack<ArrayList<Integer>> st_allMetaPaths = new Stack();
        Stack<Integer> st_currentNode = new Stack();
        Stack<Integer> st_metaPathLength = new Stack();
        st_allMetaPaths.push(pCurrentMetaPath);
        st_currentNode.push(pCurrentInstance);
        st_metaPathLength.push(pMetaPathLength);

        ArrayList<Integer> currentMetaPath;
        int currentInstance;
        int metaPathLength;

        while(!st_allMetaPaths.empty() && !st_currentNode.empty() && !st_metaPathLength.empty())
        {
            currentMetaPath = st_allMetaPaths.pop();
            currentInstance = st_currentNode.pop();
            metaPathLength = st_metaPathLength.pop();

            if (metaPathLength <= 0) {
                continue;
            }

            HashSet<AbstractMap.SimpleEntry<Integer, Integer>> outgoingEdges = new HashSet<>();
            outgoingEdges.addAll(adjacentNodesDict.get(currentInstance));
            for (AbstractMap.SimpleEntry<Integer, Integer> edge : outgoingEdges) {
                    ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                    int nodeID = edge.getKey();
                    int edgeID = edge.getValue();
                    newMetaPath.add(edgeID);
                    newMetaPath.add(nodeID);
                synchronized (duplicateFreeMetaPaths){
                        // add new meta-path to threads
                        String joinedMetaPath;
                        joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining("|"));
                        duplicateFreeMetaPaths.add(joinedMetaPath);
                        out.println(joinedMetaPath);
                    }
                    st_allMetaPaths.push(newMetaPath);
                    st_currentNode.push(nodeID);
                    st_metaPathLength.push(metaPathLength - 1);
                    //debugOut.println("finished recursion of length: " + (metaPathLength - 1));
            }
        }
        System.out.println("These are all our metapaths from node "+ pCurrentInstance);
        System.out.println(duplicateFreeMetaPaths);
    }

    private void addAndLogMetaPath(ArrayList<Integer> newMetaPath) {
        synchronized (duplicateFreeMetaPaths) {
            int oldSize = duplicateFreeMetaPaths.size();
            String joinedMetaPath = addMetaPath(newMetaPath);
            int newSize = duplicateFreeMetaPaths.size();
            if (newSize > oldSize)
                printMetaPathAndLog(joinedMetaPath);
        }
    }

    private ArrayList<Integer> copyMetaPath(ArrayList<Integer> currentMetaPath) {
        ArrayList<Integer> newMetaPath = new ArrayList<>();
        for (int label : currentMetaPath) {
            newMetaPath.add(label);
        }
        //debugOut.println("copied currentMetaPath");

        return newMetaPath;
    }

    private String addMetaPath(ArrayList<Integer> newMetaPath) {
        String joinedMetaPath;

        joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining("|"));
        duplicateFreeMetaPaths.add(joinedMetaPath);

        return joinedMetaPath;
    }

    private void printMetaPathAndLog(String joinedMetaPath) {
        out.println(joinedMetaPath);
        printCount++;
/*        if (printCount % ((int)estimatedCount/50) == 0) {
            debugOut.println("MetaPaths found: " + printCount + " estimated Progress: " + (100*printCount/estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
        }*/
    }

    public void setAdjacentNodesDict(HashMap<Integer, HashSet<AbstractMap.SimpleEntry<Integer, Integer>>> adjacentNodesDict){
        this.adjacentNodesDict = adjacentNodesDict;
    }

    //TODO -------------------------------------------------------------------

    public Stream<ComputeAllMetaPaths.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new ComputeAllMetaPaths.Result(new HashSet<>()));
    }

    @Override
    public ComputeAllMetaPathsWoExistenceCheck me() {
        return this;
    }

    @Override
    public ComputeAllMetaPathsWoExistenceCheck release() {
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
