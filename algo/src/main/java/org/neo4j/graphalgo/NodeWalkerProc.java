package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.NodeWalker;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;

public class NodeWalkerProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(name = "randomWalk", mode = Mode.READ)
    @Description("Starts a random walk of the specified number of steps at the given start node. " +
            "Optionally specify a filePath instead of returning the paths within neo4j.")
    public Stream<WalkResult> randomWalk(@Name("nodeId") long nodeId,
                                         @Name("steps") long steps,
                                         @Name("walks") long walks,
                                         @Name(value = "filePath", defaultValue = "") String filePath) throws IOException {
        NodeWalker walker = getRandomWalker();
        NodeWalker.AbstractWalkOutput output = getAppropriateOutput(walker, filePath);

        Stream<WalkResult> stream = walker.walkFromNode(output, nodeId, steps, walks);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "randomWalkFromNodeType", mode = Mode.READ)
    @Description("Starts a random walk of the specified number of steps at multiple random start nodes of the given type. " +
            "If not type is given any node is chosen. Optionally specify a filePath instead of returning the paths within neo4j.")
    public Stream<WalkResult> randomWalkFromNodeType(@Name("steps") long steps,
                                              @Name("walks") long walks,
                                              @Name(value = "type", defaultValue = "") String type,
                                              @Name(value = "filePath", defaultValue = "") String filePath) throws IOException {
        NodeWalker walker = getRandomWalker();
        NodeWalker.AbstractWalkOutput output = getAppropriateOutput(walker, filePath);

        Stream<WalkResult> stream = walker.walkFromNodeType(output, steps, walks, type);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "randomWalkFromAllNodes", mode = Mode.READ)
    @Description("Starts random walks from every node in the graph. Specify the steps for each random walks " +
            "and the number of walks per node. Optionally specify a filePath instead of returning the paths within neo4j.")
    public Stream<WalkResult> randomWalkFromAllNodes(@Name("steps") long steps,
                                                 @Name("walks") long walks,
                                                 @Name(value = "filePath", defaultValue = "") String filePath) throws IOException {
        NodeWalker walker = getRandomWalker();
        NodeWalker.AbstractWalkOutput output = getAppropriateOutput(walker, filePath);

        Stream<WalkResult> stream = walker.walkFromAllNodes(output, steps, walks);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    private HeavyGraph getGraph(){
        AllocationTracker tracker = AllocationTracker.create();
        HeavyGraph graph = (HeavyGraph) new GraphLoader(api, Pools.DEFAULT)
                .withoutNodeProperties()
                .withoutNodeWeights()
                .withoutNodeProperties()
                .withoutExecutorService()
                .withoutRelationshipWeights()
                .withAllocationTracker(tracker)
                .load(HeavyGraphFactory.class);

        return graph;
    }

    private NodeWalker.AbstractWalkOutput getAppropriateOutput(NodeWalker walker, String filePath) throws IOException{
        NodeWalker.AbstractWalkOutput output;
        if(filePath.isEmpty()){
            output = new NodeWalker.WalkDatabaseOutput(walker);
        } else {
            output = new NodeWalker.WalkNodeDirectFileOutput(filePath);
        }
        return output;
    }

    private NodeWalker getRandomWalker(){
        HeavyGraph graph = getGraph();
        NodeWalker.AbstractNextNodeStrategy nextNodeStrategy = new NodeWalker.RandomNextNodeStrategy(graph, graph);
        return new NodeWalker(graph, log, db, nextNodeStrategy);
    }

    private NodeWalker getNode2VecWalker(double returnParam, double inOutParam){
        HeavyGraph graph = getGraph();
        NodeWalker.AbstractNextNodeStrategy nextNodeStrategy = new NodeWalker.Node2VecStrategy(graph, graph, returnParam, inOutParam);
        return new NodeWalker(graph, log, db, nextNodeStrategy);
    }

    public static class WalkResult {
        public Path path;

        public WalkResult(Path path) {
            this.path = path;
        }
    }

    public static class WalkPath implements Path {

        private ArrayList<Node> nodes;
        private ArrayList<Relationship> relationships;

        public WalkPath(int size) {
            nodes = new ArrayList<>(size);
            relationships = new ArrayList<>(Math.max(0, size - 1)); // for empty paths
        }

        public void addNode(Node node) {
            nodes.add(node);
        }

        public void addRelationship(Relationship relationship) {
            relationships.add(relationship);
        }

        @Override
        public Node startNode() {
            return nodes.get(0);
        }

        @Override
        public Node endNode() {
            return nodes.get(nodes.size() - 1);
        }

        @Override
        public Relationship lastRelationship() {
            return relationships.get(relationships.size() - 1);
        }

        @Override
        public Iterable<Relationship> relationships() {
            return relationships;
        }

        @Override
        public Iterable<Relationship> reverseRelationships() {
            ArrayList<Relationship> reverse = new ArrayList<>(relationships);
            Collections.reverse(reverse);
            return reverse;
        }

        @Override
        public Iterable<Node> nodes() {
            return nodes;
        }

        @Override
        public Iterable<Node> reverseNodes() {
            ArrayList<Node> reverse = new ArrayList<>(nodes);
            Collections.reverse(reverse);
            return reverse;
        }

        @Override
        public int length() {
            return nodes.size();
        }

        @Override
        public String toString() {
            return nodes.toString();
        }

        @Override
        public Iterator<PropertyContainer> iterator() {
            //TODO ???????
            return null;
        }
    }

}