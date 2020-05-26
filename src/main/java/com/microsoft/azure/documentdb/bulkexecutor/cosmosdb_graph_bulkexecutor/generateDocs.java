/**
 * 
 */
package com.microsoft.azure.documentdb.bulkexecutor.cosmosdb_graph_bulkexecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.microsoft.azure.documentdb.bulkexecutor.graph.Element.GremlinVertex;

public class generateDocs {

	private static final String VERTEX_LABEL = "vertex";
	private static final String PARTITION_KEY_PROPERTY_NAME = "city";

	public static List<GremlinVertex> generateVertices(int numVertices)
    {
        List<GremlinVertex> vertices = new ArrayList<>();
        
        List<String> cityList = new ArrayList<>();
        cityList.add("Hyderabad");
        cityList.add("Bangalore");
        cityList.add("Delhi");

        for (int i = 0; i < numVertices; i++) {
            String id = Integer.toString(i);
            GremlinVertex v = GremlinVertex.builder()
                    .setId(id)
                    .setLabel(VERTEX_LABEL)
                    .addProperty(PARTITION_KEY_PROPERTY_NAME, cityList.get(new Random().nextInt(cityList.size())))
                    .addProperty("testProperty", UUID.randomUUID().toString())
                    .build();

            vertices.add(v);
        }
        return vertices;
    }
	
}
