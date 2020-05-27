package com.microsoft.azure.documentdb.bulkexecutor.cosmosdb_graph_bulkexecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.bulkexecutor.BulkDeleteResponse;
import com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse;
import com.microsoft.azure.documentdb.bulkexecutor.GraphBulkExecutor;
import com.microsoft.azure.documentdb.bulkexecutor.SetUpdateOperation;
import com.microsoft.azure.documentdb.bulkexecutor.UnsetUpdateOperation;
import com.microsoft.azure.documentdb.bulkexecutor.UpdateItem;
import com.microsoft.azure.documentdb.bulkexecutor.UpdateOperationBase;
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
		//Test COmments
		
		try(GraphBulkExecutor executor = graphBulkExec.build()){
			
			//Deleting all the data on the Graph
			BulkDeleteResponse dResponse = executor.deleteAll();
			System.out.println("Deleted all the Vertices and Edges in the Graph\n" + dResponse.getTotalRequestUnitsConsumed() + " RU's\n" 
					+ dResponse.getNumberOfDocumentsDeleted() + " Vertices Deleted\n");
			
			//Importing/ Inserting vertices into the graph
			BulkImportResponse iResponse = executor.importAll(new generateDocs().generateVertices(10), true, true, 10);
			System.out.println(String.format("Imported %d documents:\nRU's Consumed: %s\nErrors: %s",
					iResponse.getNumberOfDocumentsImported(), iResponse.getTotalRequestUnitsConsumed(), iResponse.getErrors()));
			
			//Add Pairs of (partitionKeyValue, id) to the ArrayList and pass it to the deleteElements to delete those combination of elements.
			ArrayList<Pair<String, String>> al = new ArrayList<>();
            Pair<String, String> p = Pair.of("Bangalore", "9");
            al.add(p);
            
            BulkDeleteResponse dResponse1 = executor.deleteElements(al);
            System.out.println(dResponse1.getNumberOfDocumentsDeleted() + " documents deleted.");
            
            //Working on trying to get the update operation working.
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
		client.close();
	}

}
