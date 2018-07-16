package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.metapath.labels.LabelMapping;

import java.util.AbstractMap;
import java.util.HashMap;

public class LabelIdToLabelNameMapping extends Algorithm<LabelIdToLabelNameMapping> {

    private HeavyGraph arrayGraphInterface;
    private LabelMapping labelMapping;

    public LabelIdToLabelNameMapping(HeavyGraph graph, LabelMapping labelMapping)
    {
        this.arrayGraphInterface = graph;
        this.labelMapping = labelMapping;
    }

    public LabelIdToLabelNameMapping.Result getLabelIdToLabelNameMapping()
    {
        return null; // todo return new Result(labelMapping.getLabels());
    }

    @Override
    public LabelIdToLabelNameMapping me() { return this; }

    @Override
    public LabelIdToLabelNameMapping release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        AbstractMap<Integer, String> labelIdToLabelNameDict;
        public Result(AbstractMap<Integer, String> labelIdToLabelNameDict) {
            this.labelIdToLabelNameDict = labelIdToLabelNameDict;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public AbstractMap<Integer, String> getLabelIdToLabelNameDict() {
            return labelIdToLabelNameDict;
        }
    }
}
