package org.neo4j.graphalgo.impl.metaPathComputation;

public class ComputeMetaPathFromNodeInstanceThread extends Thread {
    ComputeAllMetaPathsWoExistenceCheck parent;
    String threadName;
    int nodeLabel;
    int metaPathLength;

    ComputeMetaPathFromNodeInstanceThread(ComputeAllMetaPathsWoExistenceCheck parent, String threadName, int nodeLabel, int metaPathLength) {
        this.parent = parent;
        this.threadName = threadName;
        this.nodeLabel = nodeLabel;
        this.metaPathLength = metaPathLength;
    }

    public void run() {
        parent.computeMetaPathFromNodeLabel(nodeLabel, metaPathLength);
    }

    public String getThreadName() {
        return threadName;
    }
}