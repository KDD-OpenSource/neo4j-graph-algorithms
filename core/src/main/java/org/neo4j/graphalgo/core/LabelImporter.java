package org.neo4j.graphalgo.core;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Token;

import java.util.*;

public class LabelImporter extends StatementTask<List, EntityNotFoundException> {
    private final IdMap mapping;

    public LabelImporter(
            GraphDatabaseAPI api,
            IdMap mapping) {
        super(api);
        this.mapping = mapping;
    }

    @Override
    public List apply(final Statement statement) throws EntityNotFoundException, RelationshipTypeIdNotFoundKernelException {
        final ReadOperations readOp = statement.readOperations();
        Iterator<Token> labelTokens = readOp.labelsGetAllTokens();
        PrimitiveLongIterator relationships = readOp.relationshipsGetAll();

        HashMap<AbstractMap.SimpleEntry<Integer, Integer>, Integer> nodesToLabelMap = new HashMap<>();
        HashMap<Integer, String> typeToStringDictionary = new HashMap<>();
        while (relationships.hasNext()) {
            long relationshipId = relationships.next();
            readOp.relationshipVisit(relationshipId, (relationship, typeId, startNodeId, endNodeId) -> createEdgeTypeEntry(typeId, startNodeId, endNodeId, nodesToLabelMap, typeToStringDictionary, readOp));
        }

        HashMap<Integer, ArrayList<IdNameTuple>> idLabelMap = new HashMap<>();
        for (int nodeId = 0; nodeId < readOp.nodesGetCount(); nodeId++) {
            idLabelMap.put(nodeId, new ArrayList<>());
        }
        while (labelTokens.hasNext()) {
            Token token = labelTokens.next();
            PrimitiveLongIterator nodesWithThisLabel = readOp.nodesGetForLabel(token.id());
            IdNameTuple tuple = new IdNameTuple(token.id(), token.name());
            while (nodesWithThisLabel.hasNext()) {
                Integer nodeId = mapping.toMappedNodeId(nodesWithThisLabel.next());
                idLabelMap.get(nodeId).add(tuple);
            }
        }

        List result =  new ArrayList();
        result.add(idLabelMap);
        result.add(nodesToLabelMap);
        result.add(typeToStringDictionary);
        return result;
    }

    private boolean createEdgeTypeEntry(int typeId, long startNodeId, long endNodeId, HashMap<AbstractMap.SimpleEntry<Integer, Integer>, Integer> nodesToLabelMap, HashMap<Integer, String> typeToStringDictionary, ReadOperations readOp) throws RelationshipTypeIdNotFoundKernelException {
        AbstractMap.SimpleEntry<Integer, Integer> pair = new AbstractMap.SimpleEntry<>(mapping.toMappedNodeId(startNodeId), mapping.toMappedNodeId(endNodeId));
        nodesToLabelMap.put(pair, typeId);
        typeToStringDictionary.putIfAbsent(typeId, readOp.relationshipTypeGetName(typeId));
        return true;
    }

    public class IdNameTuple {
        private int id;
        private String name;

        public IdNameTuple(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public int getId() {
            return id;
        }
    }

}
