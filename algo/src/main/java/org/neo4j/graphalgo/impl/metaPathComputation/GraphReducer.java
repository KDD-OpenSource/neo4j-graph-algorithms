package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.function.Consumer;

import static java.lang.Math.toIntExact;

public class GraphReducer extends MetaPathComputation {
    public Log log;
    private GraphDatabaseService db;
    private String[] goodLabels;
    private String[] goodEdgeLabels;
    private HashSet<String> newGoodLabels = new HashSet<>();
    private HashMap<String, RelationshipType> relationshipTypeDict;
    private PrintStream debugOut;
    final int MAX_NOF_THREADS = 144;

    public GraphReducer(GraphDatabaseService db, Log log,
                        String[] goodLabels, String[] goodEdgeLabels) throws FileNotFoundException {
        this.log = log;
        this.db = db;
        this.goodEdgeLabels = goodEdgeLabels;
        this.goodLabels = goodLabels;
        this.debugOut = new PrintStream(new FileOutputStream("GraphReducer_Debug.txt"));
        relationshipTypeDict = new HashMap<>();
    }

    private void findRelationType(String edgeType) {
        RelationshipType returnType = null;
        try (Transaction transaction = db.beginTx()) {
            for (RelationshipType type : db.getAllRelationshipTypes()) {
                if (type.name().equals(edgeType)) {
                    returnType = type;
                    break;
                }
            }
            transaction.success();
            transaction.close();
        }
        relationshipTypeDict.put(edgeType, returnType);
    }

    public void compute() throws InterruptedException {
        for (String goodEdgeType : goodEdgeLabels) {
            findRelationType(goodEdgeType);
        }
        debugOut.println("created dict!");

        getTypeRelIds();
        debugOut.println("deleted edges!");

        getTypeNodeIds();
        debugOut.println("deleted nodes!");
    }

    private List<Long> getTypeRelIds() {
        LinkedList<Long> typeRelIds = new LinkedList<>();

        try (Transaction transaction = db.beginTx()) {
            ResourceIterable<Relationship> allRels = db.getAllRelationships();
            debugOut.println("getAllRelationships() worked");
            for (Relationship rel : allRels) {
                boolean shouldDelete = true;
                for (String edgeLabel : goodEdgeLabels) {
                    if (rel.getType() == relationshipTypeDict.get(edgeLabel)) {
                        shouldDelete = false;
                        break;
                    }
                }
                debugOut.println("decided if rel should be deleted");

                if (shouldDelete) {
                    deleteRelationship(rel.getId());
                    debugOut.println("deleted an edge");
                } else {
                    for (Label label : rel.getStartNode().getLabels()) {
                        newGoodLabels.add(label.name());
                    }

                    for (Label label : rel.getEndNode().getLabels()) {
                        newGoodLabels.add(label.name());
                    }
                    debugOut.println("newGoodLabelsSize: " + newGoodLabels.size());
                }
            }

            transaction.success();
            transaction.close();

            debugOut.println("Got bad edges!");
        }

        return typeRelIds;
    }


    private List<Long> getTypeNodeIds() {
        LinkedList<Long> typeNodeIds = new LinkedList<>();

        try (Transaction transaction = db.beginTx()) {
            ResourceIterable<Node> allNodes = db.getAllNodes();
            debugOut.println("getAllNodes() worked");

            for (Node node : allNodes) {
                boolean shouldDelete = true;
                for (String label : goodLabels) {
                    if (node.hasLabel(Label.label(label))) {
                        shouldDelete = false;
                        break;
                    }
                }
                debugOut.println("decided if node should be deleted1");

                if (shouldDelete) {
                    for (String label : newGoodLabels) {
                        if (node.hasLabel(Label.label(label))) {
                            shouldDelete = false;
                            break;
                        }
                    }
                }
                debugOut.println("decided if rel should be deleted2");

                if (shouldDelete) deleteNode(node.getId());
            }

            transaction.success();
            transaction.close();
            debugOut.println("Got bad nodes");
        }

        return typeNodeIds;
    }


    public boolean deleteNode(long nodeId) {
        try (Transaction tx = api.beginTx()) {
            result = api.execute("MATCH (:`" + nodeLabel1 + "`)-[:`" + edgeLabel1 + "`]-(:`" + nodeLabel2 + "`) RETURN count(*)");
            tx.success();
        }
        Map<String, Object> row = result.next();
        int countSingleTwoMP = toIntExact((long) row.get("count(*)"));
        try (Transaction transaction = db.beginTx()) {
            Node nodeInstance = db.getNodeById(nodeId);

            for (Relationship relation : nodeInstance.getRelationships(Direction.BOTH)) {
                relation.delete();
            }
            nodeInstance.delete();

            transaction.success();
            transaction.close();
            debugOut.println("deleteNode() worked");
        }

        return true;
    }

    public boolean deleteRelationship(long relId) {
        try (Transaction transaction = db.beginTx()) {
            Relationship relInstance = db.getRelationshipById(relId);
            relInstance.delete();

            transaction.success();
            transaction.close();
            debugOut.println("deleteRelationship() worked");
        }

        return true;
    }

    /* Things I don't understand */
    @Override
    public GraphReducer me() {
        return this;
    }

    @Override
    public GraphReducer release() {
        return null;
    }

    class DeleteNodesThread extends Thread {
        private Consumer<Long> deleteNode;
        private long nodeToDelete;

        DeleteNodesThread(Consumer<Long> deleteNode, long nodeToDelete) {
            this.deleteNode = deleteNode;
            this.nodeToDelete = nodeToDelete;
        }

        public void run() {
            this.deleteNode.accept(nodeToDelete);
        }
    }

    class DeleteRelationshipsThread extends Thread {
        private Consumer<Long> deleteRelationship;
        private long relationshipToDelete;

        DeleteRelationshipsThread(Consumer<Long> deleteRelationship, long relationshipToDelete) {
            this.deleteRelationship = deleteRelationship;
            this.relationshipToDelete = relationshipToDelete;
        }

        public void run() {
            this.deleteRelationship.accept(relationshipToDelete);
        }
    }

}
