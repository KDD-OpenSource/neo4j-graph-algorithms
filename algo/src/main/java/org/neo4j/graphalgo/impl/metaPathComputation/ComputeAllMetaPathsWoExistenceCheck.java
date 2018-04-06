package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.MetaPath;
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
    private HashMap<Integer, HashSet<Integer>> adjacentNodesDict = new HashMap<Integer, HashSet<Integer>>();
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
        //debugOut.println("There are " + arrayGraphInterface.getAllLabels().size() + " labels.");
        for (Node node : nodes) {
            ComputeMetaPathFromNodeLabelThread thread = new ComputeMetaPathFromNodeLabelThread(this, "thread-" + i, toIntExact(node.getId()), metaPathLength);
            thread.start();
            threads.add(thread);
            i++;
        }
        //debugOut.println("Created " + threads.size() + " threads.");
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
        //debugOut.println("\n\n" + row + "\n\n");
        //debugOut.println(rels);
    }

    private void initializeDictionaries(){
        for (Node node : nodes) {
            int nodeID = toIntExact(node.getId());
           //nodeIDLabelsDict.putIfAbsent(toIntExact(nodeID), node.getLabels().iterator().next());

            HashSet<Integer> adjNodesSet = new HashSet<Integer>();
            adjacentNodesDict.putIfAbsent(toIntExact(nodeID), adjNodesSet);
            for (Relationship rel : rels) {
                try {
                    int adjNodeID = toIntExact(rel.getOtherNodeId(node.getId()));
                    adjacentNodesDict.get(nodeID).add(adjNodeID);
                } catch (Exception e) {}
            }
        }
    }

    public void computeMetaPathFromNodeLabel(int nodeID, int metaPathLength) { //TODO will it be faster if not node but nodeID with dicts?
        ArrayList<Integer> initialMetaPath = new ArrayList<>();
        initialMetaPath.add(nodeID); //because node is already type (for real graph)
        computeMetaPathFromNodeLabel(initialMetaPath, nodeID, metaPathLength - 1);
        //debugOut.println("finished recursion for: " + startNodeLabel);
    }

    private void computeMetaPathFromNodeLabel(ArrayList<Integer> pCurrentMetaPath, int pCurrentInstance, int pMetaPathLength) {
        Stack<ArrayList<Integer>> param1 = new Stack();
        Stack<Integer> param2 = new Stack();
        Stack<Integer> param3 = new Stack();
        param1.push(pCurrentMetaPath);
        param2.push(pCurrentInstance);
        param3.push(pMetaPathLength);

        ArrayList<Integer> currentMetaPath;
        int currentInstance;
        int metaPathLength;

        while(!param1.empty() && !param2.empty() && !param3.empty())
        {
            currentMetaPath = param1.pop();
            currentInstance = param2.pop();
            metaPathLength = param3.pop();

            if (metaPathLength <= 0) {
                //debugOut.println("aborting recursion");
                continue;
            }

            //debugOut.println(((ComputeMetaPathFromNodeLabelThread) Thread.currentThread()).getThreadName() + ": Length of currentInstances: " + currentInstances.size());
            //debugOut.println(Thread.currentThread().getName() + ": MetaPathLength: " + metaPathLength);
            //debugOut.println(Thread.currentThread().getName() + ": _________________");

            HashSet<Integer> nextInstances = new HashSet<>();
            fillNextInstances(currentInstance, nextInstances);
            //debugOut.println(((ComputeMetaPathFromNodeLabelThread) Thread.currentThread()).getThreadName() + ": Time for next instanceCalculation: " + (endTime - startTime));
            Iterator<Integer> iterator = nextInstances.iterator();
            for (int i = 0; i < nextInstances.size(); i++) {
                if (!nextInstances.isEmpty()) {
                    ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                    int nextInstance = iterator.next();
                    newMetaPath.add(nextInstance);

                    addAndLogMetaPath(newMetaPath);
                    nextInstances = null; // how exactly does this work?

                    param1.push(newMetaPath);
                    param2.push(nextInstance);
                    param3.push(metaPathLength - 1);
                     debugOut.println("finished recursion of length: " + (metaPathLength - 1));
                }
            }
        }
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

    private String addMetaPath(ArrayList<Integer> newMetaPath) {
        String joinedMetaPath;

        joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining(" | "));
        duplicateFreeMetaPaths.add(joinedMetaPath);
        //debugOut.println("tried adding new Metapath");

        return joinedMetaPath;
    }

    private void printMetaPathAndLog(String joinedMetaPath) {
        out.println(joinedMetaPath);
        printCount++;
        if (printCount % ((int)estimatedCount/50) == 0) {
            debugOut.println("MetaPaths found: " + printCount + " estimated Progress: " + (100*printCount/estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
        }
    }

    private void fillNextInstances(int currentInstance, HashSet<Integer> nextInstances) {
        nextInstances.addAll(adjacentNodesDict.get(currentInstance));
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
