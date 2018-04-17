package org.neo4j.graphalgo.results;

import java.util.HashSet;
import java.util.Vector;


public class ComputeAllMetaPathsWoExistenceCheckResult {

    public final String metaPaths;

    private ComputeAllMetaPathsWoExistenceCheckResult(Vector<String> metaPaths) {
        this.metaPaths = "";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ComputeAllMetaPathsWoExistenceCheckResult> {

        private HashSet<String> metaPaths;

        public void setMetaPaths(HashSet<String> metaPaths) {
            // this.metaPaths =  metaPaths.toArray(new String[metaPaths.size()]);
            this.metaPaths = metaPaths;
        }

        public ComputeAllMetaPathsWoExistenceCheckResult build() {
            return new ComputeAllMetaPathsWoExistenceCheckResult(new Vector<>(metaPaths));

        }
    }
}