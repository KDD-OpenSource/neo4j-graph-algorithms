package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.metapath.labels.GraphLabeler;
import org.neo4j.graphalgo.impl.metapath.labels.Tokens;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MetaPathInstances extends AbstractWalkAlgorithm {

    private BoundedExecutor executor;
    private GraphLabeler graphLabeler;
    private Phaser phaser;

    public MetaPathInstances(HeavyGraph graph, Log log, GraphLabeler graphLabeler){
        super(graph, log);

        this.graphLabeler = graphLabeler;
        this.executor = getBoundedExecutor();
        this.phaser = new Phaser();

    }

    public Stream<WalkResult> findMetaPathInstances(String metaPath, AbstractWalkOutput output){
        int[] emptyArray = {};
        int[] types = parseMetaPath(metaPath);

        for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            continueWithNextNode(nodeId, emptyArray, types, output);
        }

        phaser.awaitAdvance(0);
        executor.getExecutor().shutdown();
        System.out.println("Starting waiting for threads");
        try {
            executor.getExecutor().awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Thread join timed out");
        }
        System.out.println("Ended waiting for threads");

        output.endInput();

        return output.getStream();
    }

    private int[] parseMetaPath(String metaPath){
        String[] stringTypes = metaPath.split("%%");
        int[] typeIds = new int[stringTypes.length];


        Tokens labelTokens = graphLabeler.getLabels();
        Tokens typeTokens = graphLabeler.getTypes();
        for(int i = 0; i < stringTypes.length; i++){
            String stringType = stringTypes[i];
            int typeId;
            if(i % 2 == 0){
                typeId = labelTokens.getId(stringType);
            } else {
                typeId = typeTokens.getId(stringType);
            }
            typeIds[i] = typeId;
        }
        return typeIds;
    }

    private int[] recurse(int nodeId, short[] metaPath, int position) {

        graphLabeler.getNodesForLabel(startLabel) // parallelize over these
        if(!graphLabeler.hasLabel(nodeId, metaPath[position])) return null;

        int nextPosition = position + 2;

        int[] neighbours = graph.getAdjacentNodes(nodeId);
        for (int neighbourId : neighbours) {
            if (graphLabeler.hasLabel(neighbourId, metaPath[nextPosition]) &&
                    graphLabeler.getEdgeLabel(nodeId, neighbourId) == metaPath[position + 1]) {
                int[] result;
                if (nextPosition == metaPath.length - 1) {
                    result = new int[metaPath.length / 2 + 1];
                } else {
                    result = recurse(neighbourId, metaPath, nextPosition);
                }
                if (result != null) {
                    result[position / 2] = neighbourId;
                }
                return result;
            }
        }
        return null;
    }
    private void continueWithNextNode(final int nodeId, final int[] previousResults, final int[] types, final AbstractWalkOutput output){
        int typeIndex = previousResults.length * 2; // types array contains types for edges and nodes, prev-array contains only node ids
        int currentNodeType = types[typeIndex];
        // End this walk if it doesn't match the required types anymore, a label of -1 means no label is attached to this node
        if(!graphLabeler.hasLabel(nodeId, (short)currentNodeType)) return;

        int[] resultsSoFar = arrayIntPush(nodeId, previousResults);

        // If this walk completes the required types, end it and save it
        if(typeIndex + 1 == types.length){
            output.addResult(translateIdsToOriginal(resultsSoFar));
            return;
        }

        startEdgeCheck(nodeId, resultsSoFar, types, output);
    }

    public void startEdgeCheck(final int nodeId, final int[] previousResults, final int[] types, final AbstractWalkOutput output){
        try {
            executor.submitTask(() -> {
                phaser.register(); // Do not end computation while any extraction is still running
                checkEdges(nodeId, previousResults, types, output);
                phaser.arriveAndDeregister();
            });
        } catch (InterruptedException e){
            log.error("Thread waiting timed out");
        }
    }

    public void checkEdges(final int nodeId, final int[] previousResults, final int[] types, final AbstractWalkOutput output){
        int edgeIndex = previousResults.length * 2 - 1;
        int nextEdgeType = types[edgeIndex];

        int[] neighbours = graph.getAdjacentNodes(nodeId);
        for(int i = 0; i < neighbours.length; i++){
            int neighbourId = neighbours[i];
            int edgeType = graphLabeler.getEdgeLabel(nodeId, neighbourId);

            if(edgeType == nextEdgeType){
                continueWithNextNode(neighbourId, previousResults, types, output);
            }
        }
    }

    private static int[] arrayIntPush(int item, int[] oldArray) {
        int[] result = Arrays.copyOf(oldArray, oldArray.length + 1);
        result[oldArray.length] = item;
        return result;
    }

}
