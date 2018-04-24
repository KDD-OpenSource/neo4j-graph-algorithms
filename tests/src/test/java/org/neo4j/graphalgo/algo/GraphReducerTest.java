package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.metaPathComputationProcs.GraphReducerProc;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashSet;

import static org.junit.Assert.assertTrue;

public class GraphReducerTest {

    private static GraphDatabaseAPI api;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
                "CREATE (s:Node {name:'s'})\n" +
                        "CREATE (b:A {name:'b'})\n" +
                        "CREATE (c:A {name:'c'})\n" +
                        "CREATE (d:A {name:'d'})\n" +

                        "CREATE (e:B {name:'e'})\n" +
                        "CREATE (f:B {name:'f'})\n" +
                        "CREATE (g:B {name:'g'})\n" +
                        "CREATE (h:B {name:'h'})\n" +

                        "CREATE" +
                        " (e)-[:NORMAL {cost:5}]->(b),\n" +
                        " (e)-[:NORMAL {cost:5}]->(c),\n" +
                        " (e)-[:NORMAL {cost:3}]->(d),\n" +
                        " (e)-[:NORMAL {cost:3}]->(f),\n" +
                        " (e)-[:NORMAL {cost:3}]->(g),\n" +
                        " (e)-[:NORMAL {cost:3}]->(h),\n" +

                        " (b)-[:NORMAL {cost:3}]->(d),\n" +
                        " (b)-[:SPECIAL {cost:3}]->(f),\n" +

                        " (c)-[:NORMAL {cost:3}]->(g),\n" +
                        " (c)-[:NORMAL {cost:3}]->(h),\n" +

                        " (d)-[:NORMAL {cost:3}]->(f),\n" +

                        " (g)-[:SPECIAL {cost:3}]->(h)";

        api = TestDatabaseCreator.createTestDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(GraphReducerProc.class);

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() {
        api.shutdown();
    }

    @Test
    public void testGettingStarted() {
        final String cypher = "CALL algo.graphReducer(['SPECIAL'], ['A']) YIELD success, executionTime";
        final HashSet<String> expectedNodes = new HashSet<>();
        expectedNodes.add("b");
        expectedNodes.add("d");
        expectedNodes.add("f");
        expectedNodes.add("g");
        expectedNodes.add("c");
        expectedNodes.add("h");

        api.execute(cypher);
        try (Transaction tx = api.beginTx()) {
            for (Node node : api.getAllNodes()) {
                assertTrue(
                        "Only Special Nodes and their neighbors should remain in Graph",
                        expectedNodes.contains((String) node.getProperty("name")));
            }
            tx.success();
        }
    }
}
