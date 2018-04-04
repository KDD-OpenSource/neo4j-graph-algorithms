package org.neo4j.graphalgo.core;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Token;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class LabelImporter extends StatementTask<HashMap<Integer, ArrayList<Object>>, EntityNotFoundException> {
    private final IdMap mapping;

    public LabelImporter(
            GraphDatabaseAPI api,
            IdMap mapping) {
        super(api);
        this.mapping = mapping;
    }

    @Override
    public HashMap<Integer, ArrayList<Object>> apply(final Statement statement) throws
            EntityNotFoundException {

        final ReadOperations readOp = statement.readOperations();

        Iterator<Token> labelTokens = readOp.labelsGetAllTokens();
        Iterator<Token> relationshipTokens = readOp.relationshipTypesGetAllTokens();

        while (relationshipTokens.hasNext()){
            Token token = relationshipTokens.next();
            PrimitiveLongIterator nodes =  readOp.nodesGetAll();
            readOp.nodeGetRelationships(0, Direction.BOTH).next();
            readOp.nodeGetRelationshipTypes(0);
            readOp.relationshipsGetAll();
            RawValues.combineIntInt(Direction.BOTH, 0, 0);
        }

        HashMap<Integer, ArrayList<Object>> idLabelMap = new HashMap<>();
        while (labelTokens.hasNext()){
            Token token = labelTokens.next();
            PrimitiveLongIterator nodesWithThisLabel = readOp.nodesGetForLabel(token.id());
            while (nodesWithThisLabel.hasNext()){
                ArrayList<Object> idAndName = new ArrayList<>();
                idAndName.add(token.id());
                idAndName.add(token.name());
                idLabelMap.put(mapping.toMappedNodeId(nodesWithThisLabel.next()), idAndName);
            }
        }

        return idLabelMap;
    }

}
