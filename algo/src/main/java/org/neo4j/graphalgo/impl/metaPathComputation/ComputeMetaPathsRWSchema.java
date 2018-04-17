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

public class ComputeMetaPathsRWSchema extends Algorithm<ComputeMetaPathsRWSchema> {

    private int randomWalkLength;
    private int numberOfrandomWalks;
    private ArrayList<ArrayList<Integer>> metapaths;
    private ArrayList<Integer> metapathsWeights;
    private Random random;
    private final static int DEFAULT_WEIGHT = 5;
    private PrintStream debugOut;
    public GraphDatabaseAPI api;
    private HashMap<Integer, HashSet<Integer>> adjacentNodesDict = new HashMap<Integer, HashSet<Integer>>();
    private HashMap<Label, Integer> nodeLabelsIDDict = new HashMap<Label, Integer>();
    private HashMap<Integer, Integer> nodeLabelsDict = new HashMap<Integer, Integer>();

    public ComputeMetaPathsRWSchema(
                                               int numberOfRandomWalks,
                                               int randomWalkLength, GraphDatabaseAPI api) throws Exception {


        this.numberOfrandomWalks = numberOfRandomWalks;
        this.randomWalkLength = randomWalkLength;
        this.metapaths = new ArrayList<>();
        this.metapathsWeights = new ArrayList<>();
        this.random = new Random();
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Debug_2.txt"));
        this.api = api;
    }

    public Result compute() {
        debugOut.println("START");
        //getMetaNodesEdges
        org.neo4j.graphdb.Result schema = null;
        org.neo4j.graphdb.Result result = null;
        try (Transaction tx = api.beginTx()) {
            result = api.execute("CALL apoc.meta.graph()");
            tx.success();
        }
        Map<String, Object> row = result.next();
        List<Node> nodes = (List<Node>) row.get("nodes");
        List<Relationship> rels = (List<Relationship>) row.get("relationships");
        debugOut.println("\n\n" + row + "\n\n");
        debugOut.println(rels);
        //dictionary with node id -> adjacent node id
        //createDicts
        for (Node node : nodes){
            nodeLabelsIDDict.putIfAbsent(node.getLabels().iterator().next(), toIntExact(node.getId()));
        }

        for (Node node : nodes) {
            int nodeID = toIntExact(node.getId());
            HashSet<Integer> adjNodesSet = new HashSet<Integer>();
            adjacentNodesDict.putIfAbsent(toIntExact(nodeID), adjNodesSet);
            nodeLabelsDict.putIfAbsent(toIntExact(nodeID), nodeLabelsIDDict.get(node.getLabels().iterator().next()));//TODO nodeID -> nodeID ???
            for (Relationship rel : rels) {
                try {
                    int adjNodeID = toIntExact(rel.getOtherNodeId(node.getId()));
                    adjacentNodesDict.get(nodeID).add(adjNodeID);
                } catch (Exception e) {}
            }
        }

        //compute
        for (Node node : nodes) {
            computeMetapathFromNode(toIntExact(node.getId()));
        }

        //finalize
        HashSet<String> finalMetaPaths = new HashSet<>();

        for (ArrayList<Integer> metaPath : metapaths) {
            finalMetaPaths.add(metaPath.stream().map(Object::toString).collect(Collectors.joining(" | ")) + "\n");
        }

        debugOut.println(finalMetaPaths);
        return new Result(finalMetaPaths);
    }

    private void computeMetapathFromNode(int startNodeId) {
        for (int i = 0; i < numberOfrandomWalks; i++) {
            int nodeHopId = startNodeId;
            ArrayList<Integer> metapath = new ArrayList<>();
            metapath.add(nodeLabelsDict.get(nodeHopId));
            for (int j = 1; j <= randomWalkLength; j++) {
                if (adjacentNodesDict.get(nodeHopId).size() <= 0) {
                    break;
                } else {
                    int randomEdgeIndex = random.nextInt(adjacentNodesDict.get(nodeHopId).size()); //TODO degree instead
                    nodeHopId = adjacentNodesDict.get(nodeHopId).toArray(new Integer[adjacentNodesDict.get(nodeHopId).size()])[randomEdgeIndex]; //TODO TEST!!!
                    metapath.add(nodeLabelsDict.get(nodeHopId));
                }
            }
            metapaths.add(metapath);
            metapathsWeights.add(DEFAULT_WEIGHT);
        }
    }

    public Stream<ComputeMetaPathsRWSchema.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new ComputeMetaPathsRWSchema.Result(new HashSet<>()));
    }

    @Override
    public ComputeMetaPathsRWSchema me() {
        return this;
    }

    @Override
    public ComputeMetaPathsRWSchema release() {
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
