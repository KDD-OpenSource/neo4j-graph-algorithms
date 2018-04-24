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

    public GraphReducer(GraphDatabaseService db, Log log, String[] goodLabels, String[] goodEdgeLabels) throws FileNotFoundException {
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

        List<Long> idsOfDeletablbeRels = getTypeRelIds();
        List<List<Long>> listsForThreads = new ArrayList<>(MAX_NOF_THREADS);
        for (int i = 0; i < MAX_NOF_THREADS; i++) {
            listsForThreads.add(new ArrayList<>());
        }
        int index = 0;
        for (long relId : idsOfDeletablbeRels) {
            listsForThreads.get(index % MAX_NOF_THREADS).add(relId);
            index++;
        }
        debugOut.println("prepared ThreadLists");

        List<Thread> threads = new ArrayList<>(MAX_NOF_THREADS);
        for (int i = 0; i < MAX_NOF_THREADS; i++)
        {
            DeleteRelationshipsThread thread = new DeleteRelationshipsThread(this::deleteRelationship, listsForThreads.get(i));
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
        threads.clear();

        debugOut.println("deleted edges!");

        List<Long> idsOfDeletablbeNodes = getTypeNodeIds();
        listsForThreads.clear();
        for (int i = 0; i < MAX_NOF_THREADS; i++) {
            listsForThreads.add(new ArrayList<>());
        }
        index = 0;
        for (long nodeId : idsOfDeletablbeNodes) {
            listsForThreads.get(index % MAX_NOF_THREADS).add(nodeId);
            index++;
        }
        debugOut.println("prepared ThreadLists2");

        for (int i = 0; i < MAX_NOF_THREADS; i++)
        {
            DeleteNodesThread thread = new DeleteNodesThread(this::deleteNode, listsForThreads.get(i));
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
        threads.clear();

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
                    typeRelIds.add(rel.getId());
                    debugOut.println("added an edge");
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

                if (shouldDelete) typeNodeIds.add(node.getId());
            }

            transaction.success();
            transaction.close();
            debugOut.println("Got bad nodes");
        }

        return typeNodeIds;
    }

    public boolean deleteNode(long nodeId) {
        Node node = db.getNodeById(nodeId);
        for (Relationship rel : node.getRelationships(Direction.BOTH)) {
            rel.delete();
        }
        node.delete();
        debugOut.println("deleteNode() worked");

        return true;
    }

    public boolean deleteRelationship(long relId) {
        db.getRelationshipById(relId).delete();
        debugOut.println("deleteRelationship() worked");
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
        private List<Long> nodesToDelete;

        DeleteNodesThread(Consumer<Long> deleteNode, List<Long> nodesToDelete) {
            this.deleteNode = deleteNode;
            this.nodesToDelete = nodesToDelete;
        }

        public void run() {
            try (Transaction transaction = db.beginTx()) {
                for (long nodeId : nodesToDelete) {
                    this.deleteNode.accept(nodeId);
                }

                transaction.success();
                transaction.close();
                debugOut.println("deletedAllNodes in this thread");
            }
        }
    }

    class DeleteRelationshipsThread extends Thread {
        private Consumer<Long> deleteRelationship;
        private List<Long> relationshipsToDelete;

        DeleteRelationshipsThread(Consumer<Long> deleteRelationship, List<Long> relationshipsToDelete) {
            this.deleteRelationship = deleteRelationship;
            this.relationshipsToDelete = relationshipsToDelete;
        }

        public void run() {
            try (Transaction transaction = db.beginTx()) {
                for (long relId : relationshipsToDelete) {
                    this.deleteRelationship.accept(relId);
                }

                transaction.success();
                transaction.close();
                debugOut.println("deletedAllRelationships in this thread");
            }
        }
    }

}
