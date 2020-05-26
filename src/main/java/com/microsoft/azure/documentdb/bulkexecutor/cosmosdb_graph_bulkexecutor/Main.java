package com.microsoft.azure.documentdb.bulkexecutor.cosmosdb_graph_bulkexecutor;

import java.util.ArrayList;

import org.apache.commons.lang3.tuple.Pair;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.bulkexecutor.BulkDeleteResponse;
import com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse;
import com.microsoft.azure.documentdb.bulkexecutor.GraphBulkExecutor;
import com.microsoft.azure.documentdb.DocumentCollection;

public class Main {

	public static String endpoint = "<ENDPOINT_FROM_PORTAL>";
	public static String password = "<PASSWORD_FROM_PORTAL>";
	public static String dbName = "<dbname>";
	public static String collName = "<collectionname>";
	public static void main(String[] args) throws DocumentClientException {
		System.out.println("Running Bulkexecutor for Gremlin API CosmosDB");
		new Main().bulkExecutorGremlin();
	}
	
	public void bulkExecutorGremlin() throws DocumentClientException
	{
		ConnectionPolicy policy = new ConnectionPolicy();
		policy.setConnectionMode(ConnectionMode.DirectHttps);
		policy.getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(10);
		policy.getRetryOptions().setMaxRetryWaitTimeInSeconds(30);
		
		DocumentClient client = new DocumentClient(endpoint, password, policy, ConsistencyLevel.Session);
		
		DocumentCollection collection = client.readCollection(String.format("/dbs/%s/colls/%s", dbName, collName), null).getResource();		
		GraphBulkExecutor.Builder graphBulkExec = GraphBulkExecutor.builder().from(client, dbName, collName, collection.getPartitionKey(),400);
		
		try(GraphBulkExecutor executor = graphBulkExec.build()){
			
			BulkDeleteResponse dResponse = executor.deleteAll();
			System.out.println("Deleted all the Vertices and Edges in the Graph\n" + dResponse.getTotalRequestUnitsConsumed() + " RU's\n" 
					+ dResponse.getNumberOfDocumentsDeleted() + " Vertices Deleted\n");
			
			BulkImportResponse iResponse = executor.importAll(new generateDocs().generateVertices(10), true, true, 10);
			System.out.println(String.format("Imported %d documents:\nRU's Consumed: %s\nErrors: %s",
					iResponse.getNumberOfDocumentsImported(), iResponse.getTotalRequestUnitsConsumed(), iResponse.getErrors()));
			
			
			ArrayList<Pair<String, String>> al = new ArrayList<>();
            Pair<String, String> p = Pair.of("Bangalore", "9");
            al.add(p);
            
            BulkDeleteResponse dResponse1 = executor.deleteElements(al);
            System.out.println(dResponse1.getNumberOfDocumentsDeleted() + " documents deleted.");
            
            
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
		client.close();
	}

}
