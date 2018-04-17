package org.neo4j.graphalgo.results;

import java.util.HashSet;
import java.util.Vector;


public class ComputeMetaPathsRWSchemaResult {

    public final String metaPaths;

    private ComputeMetaPathsRWSchemaResult(Vector<String> metaPaths) {
        this.metaPaths = "";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ComputeMetaPathsRWSchemaResult> {

        private HashSet<String> metaPaths;

        public void setMetaPaths(HashSet<String> metaPaths) {
            // this.metaPaths =  metaPaths.toArray(new String[metaPaths.size()]);
            this.metaPaths = metaPaths;
        }

        public ComputeMetaPathsRWSchemaResult build() {
            return new ComputeMetaPathsRWSchemaResult(new Vector<>(metaPaths));

        }
    }
}