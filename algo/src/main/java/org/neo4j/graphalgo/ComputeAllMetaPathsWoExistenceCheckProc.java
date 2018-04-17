package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsWoExistenceCheck;
import org.neo4j.graphalgo.results.ComputeAllMetaPathsWoExistenceCheckResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashSet;
import java.util.stream.Stream;

public class ComputeAllMetaPathsWoExistenceCheckProc {


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.computeAllMetaPathsWoCheck")
    @Description("CALL algo.computeAllMetaPathsWoCheck(length:int) YIELD length: \n" +
            "Precomputes all metapaths up to a metapath-length given by 'length' and saves them to a File called 'Precomputed_MetaPaths.txt' \n")

    public Stream<ComputeAllMetaPathsWoExistenceCheckResult> ComputeAllMetaPathsWoExistenceCheck(
            @Name(value = "length", defaultValue = "5") String lengthString) throws Exception {
        int length = Integer.valueOf(lengthString);

        final ComputeAllMetaPathsWoExistenceCheckResult.Builder builder = ComputeAllMetaPathsWoExistenceCheckResult.builder();

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        final ComputeAllMetaPathsWoExistenceCheck algo = new ComputeAllMetaPathsWoExistenceCheck(length, api);
        HashSet<String> metaPaths;
        metaPaths = algo.compute().getFinalMetaPaths();
        builder.setMetaPaths(metaPaths);
        graph.release();
        //return algo.resultStream();
        //System.out.println(Stream.of(builder.build()));
        return Stream.of(builder.build());
    }
}