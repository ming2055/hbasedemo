package com.elasticsearch.documentAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class BulkAPI {
    public static void main(String[] args) throws IOException, InterruptedException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));


        //The Bulk API supports only documents encoded in JSON or SMILE. Providing documents in any other format will result in an error.

        //A BulkRequest can be used to execute multiple index, update and/or delete operations using a single request.
        //
        //It requires at least one operation to be added to the Bulk request:
//        BulkRequest request = new BulkRequest();
//        request.add(new IndexRequest("posts").id("1")
//                .source(XContentType.JSON,"field", "foo"));
//        request.add(new IndexRequest("posts").id("2")
//                .source(XContentType.JSON,"field", "bar"));
//        request.add(new IndexRequest("posts").id("3")
//                .source(XContentType.JSON,"field", "baz"));

        //And different operation types can be added to the same BulkRequest:
        BulkRequest request1 = new BulkRequest();
        request1.add(new DeleteRequest("posts", "3"));
        request1.add(new UpdateRequest("posts", "2")
                .doc(XContentType.JSON, "other", "test"));
        request1.add(new IndexRequest("posts").id("4")
                .source(XContentType.JSON, "field", "baz"));


        BulkResponse bulkResponse = client.bulk(request1, RequestOptions.DEFAULT);
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            switch (bulkItemResponse.getOpType()) {
                case INDEX:
                case CREATE:
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                    System.out.println(deleteResponse.toString());
            }
        }

        if (bulkResponse.hasFailures()) {
            System.out.println("The Bulk response provides a method to quickly check if one or more operation has failed");
        }


        //BulkProcessor The BulkProcessor simplifies the usage of the Bulk API by providing a utility class that allows index/update/delete operations to be transparently executed as they are added to the processor.

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
//                logger.debug("Executing bulk [{}] with {} requests",
//                        executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (response.hasFailures()) {
//                    logger.warn("Bulk [{}] executed with failures", executionId);
                } else {
//                    logger.debug("Bulk [{}] completed in {} milliseconds",
//                            executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  Throwable failure) {
//                logger.error("Failed to execute bulk", failure);
            }
        };

        BulkProcessor bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener).build();

//        BulkProcessor.Builder builder = BulkProcessor.builder(
//                (request, bulkListener) ->
//                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
//                listener);
//        builder.setBulkActions(500);
//        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
//        builder.setConcurrentRequests(0);
//        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
//        builder.setBackoffPolicy(BackoffPolicy
//                .constantBackoff(TimeValue.timeValueSeconds(1L), 3));


        IndexRequest one = new IndexRequest("posts").id("1")
                .source(XContentType.JSON, "title",
                        "In which order are my Elasticsearch queries executed?");
        IndexRequest two = new IndexRequest("posts").id("2")
                .source(XContentType.JSON, "title",
                        "Current status and upcoming changes in Elasticsearch");
        IndexRequest three = new IndexRequest("posts").id("3")
                .source(XContentType.JSON, "title",
                        "The Future of Federated Search in Elasticsearch");

        bulkProcessor.add(one);
        bulkProcessor.add(two);
        bulkProcessor.add(three);

        boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);

        bulkProcessor.close();

        client.close();
    }
}
