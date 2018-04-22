package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class GraphReducer extends MetaPathComputation {
    public Log log;
    private GraphDatabaseService db;
    private String[] goodLabels;
    private String[] goodEdgeLabels;
    private ArrayList<String> newGoodLabels = new ArrayList<>();
    private HashMap<String, RelationshipType> relationshipTypeDict;
    private PrintStream debugOut;
    final int MAX_NOF_THREADS = 144; //TODO why not full utilization?
    final Semaphore threadSemaphore = new Semaphore(MAX_NOF_THREADS);


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
        }
        relationshipTypeDict.put(edgeType, returnType);
    }

    public void compute() throws InterruptedException {
        LinkedList<Thread> threads = new LinkedList<>();
        for (String goodEdgeType : goodEdgeLabels) {
            findRelationType(goodEdgeType);
        }
        debugOut.println("created dict!");

        for (long relId : getTypeRelIds()) {
            threadSemaphore.acquire();
            Thread thread = new DeleteRelationshipsThread(this::deleteRelationship, relId);
            threads.add(thread);
            thread.run();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (Exception e) {
                log.error(e.getLocalizedMessage());
            }
        }
        threads.clear();

        debugOut.println("deleted edges!");

        for (long nodeId : getTypeNodeIds()) {
            threadSemaphore.acquire();
            Thread thread = new DeleteNodesThread(this::deleteNode, nodeId);
            threads.add(thread);
            thread.run();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (Exception e) {
                log.error(e.getLocalizedMessage());
            }
        }
        debugOut.println("deleted nodes!");

    }

    private List<Long> getTypeRelIds() {
        LinkedList<Long> typeRelIds = new LinkedList<>();

        try (Transaction transaction = db.beginTx()) {
            ResourceIterable<Relationship> allRels = db.getAllRelationships();
            for (Relationship rel : allRels) {
                boolean shouldDelete = true;
                for (String edgeLabel : goodEdgeLabels) {
                    if (rel.getType() == relationshipTypeDict.get(edgeLabel)) {
                        shouldDelete = false;
                        break;
                    }
                }

                if (shouldDelete) {
                    typeRelIds.add(rel.getId());
                } else {
                    for (Label label : rel.getStartNode().getLabels()) {
                        newGoodLabels.add(label.name());
                    }

                    for (Label label : rel.getEndNode().getLabels()) {
                        newGoodLabels.add(label.name());
                    }

                }
            }

            transaction.success();
            debugOut.println("Got bad edges!");
        }

        return typeRelIds;
    }


    private List<Long> getTypeNodeIds() {
        LinkedList<Long> typeNodeIds = new LinkedList<>();

        try (Transaction transaction = db.beginTx()) {
            ResourceIterable<Node> allNodes = db.getAllNodes();
            for (Node node : allNodes) {
                boolean shouldDelete = true;
                for (String label : goodLabels) {
                    if (node.hasLabel(Label.label(label))) {
                        shouldDelete = false;
                        break;
                    }
                }

                if (shouldDelete) {
                    for (String label : newGoodLabels) {
                        if (node.hasLabel(Label.label(label))) {
                            shouldDelete = false;
                            break;
                        }
                    }
                }
                if (shouldDelete) typeNodeIds.add(node.getId());
            }

            transaction.success();
            debugOut.println("Got bad nodes");
        }

        return typeNodeIds;
    }


    public boolean deleteNode(long nodeId) {
        try (Transaction transaction = db.beginTx()) {
            Node nodeInstance = db.getNodeById(nodeId);

            for (Relationship relation : nodeInstance.getRelationships(Direction.BOTH)) {
                relation.delete();
            }
            nodeInstance.delete();

            threadSemaphore.release();
            transaction.success();
        }

        return true;
    }

    public boolean deleteRelationship(long relId) {
        try (Transaction transaction = db.beginTx()) {
            Relationship relInstance = db.getRelationshipById(relId);
            relInstance.delete();

            threadSemaphore.release();
            transaction.success();
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