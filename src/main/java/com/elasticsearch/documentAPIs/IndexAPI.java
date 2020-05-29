package com.elasticsearch.documentAPIs;


import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class IndexAPI {

    public static void main(String[] args) {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));

        //index api
        //index request
        //index
        IndexRequest indexRequest1 = new IndexRequest("posts");
        //document id
        indexRequest1.id("1");
        //Document source provided as a String
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        indexRequest1.source(jsonString, XContentType.JSON);

        //Providing the document source
        //The document source can be provided in different ways in addition to the String example shown above:
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        //Document source provided as a Map which gets automatically converted to JSON format
        IndexRequest indexRequest2 = new IndexRequest("posts")
                .id("2").source(jsonMap);


        //Document source provided as an XContentBuilder object, the Elasticsearch built-in helpers to generate JSON content
        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "kimchy");
                builder.timeField("postDate", new Date());
                builder.field("message", "trying out Elasticsearch");
            }
            builder.endObject();
            IndexRequest indexRequest3 = new IndexRequest("posts")
                    .id("3").source(builder);
        } catch (IOException e) {
            e.printStackTrace();
        }


        //Document source provided as Object key-pairs, which gets converted to JSON format
        IndexRequest indexRequest4 = new IndexRequest("posts")
                .id("4")
                .source("user", "kimchy",
                        "postDate", new Date(),
                        "message", "trying out Elasticsearch");


        //Optional argumentsedit
        //The following arguments can optionally be provided:
        // Routing value
        indexRequest4.routing("routing");
        //Timeout to wait for primary shard to become available as a TimeValue
        indexRequest4.timeout(TimeValue.timeValueSeconds(1));
        // Timeout to wait for primary shard to become available as a String
        indexRequest4.timeout("1s");
        //Refresh policy as a WriteRequest.RefreshPolicy instance
        indexRequest4.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        // Refresh policy as a String
        indexRequest4.setRefreshPolicy("wait_for");
        //Version
        indexRequest4.version(2);
        //Version type
        indexRequest4.versionType(VersionType.EXTERNAL);
        //Operation type provided as an DocWriteRequest.OpType value
        indexRequest4.opType(DocWriteRequest.OpType.CREATE);
        // Operation type provided as a String: can be create or update (default)
        indexRequest4.opType("create");
        // The name of the ingest pipeline to be executed before indexing the document
        indexRequest4.setPipeline("pipeline");


        // Synchronous Execution
        // When executing a IndexRequest in the following manner,
        // the client waits for the IndexResponse to be returned before continuing with code execution:
        try {
            IndexResponse indexResponse1 = client.index(indexRequest1, RequestOptions.DEFAULT);
            //The returned IndexResponse allows to retrieve information about the executed operation as follows:
            String index = indexResponse1.getIndex();
            String id = indexResponse1.getId();
            if (indexResponse1.getResult() == DocWriteResponse.Result.CREATED) {
                //Handle (if needed) the case where the document was created for the first time
                System.out.println("=====created=====");
            } else if (indexResponse1.getResult() == DocWriteResponse.Result.UPDATED) {
                //Handle (if needed) the case where the document was rewritten as it was already existing
                System.out.println("=====UPDATED=====");
            }
            ReplicationResponse.ShardInfo shardInfo = indexResponse1.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                //Handle the situation where number of successful shards is less than total shards

                System.out.println("======number of successful shards is less than total shards===");
            }
            if (shardInfo.getFailed() > 0) {
                //Handle the potential failures
                for (ReplicationResponse.ShardInfo.Failure failure :
                        shardInfo.getFailures()) {
                    String reason = failure.reason();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //If there is a version conflict, an ElasticsearchException will be thrown:
//        IndexRequest request1 = new IndexRequest("posts")
//                .id("1")
//                .source("field", "value")
//                .setIfSeqNo(10L)
//                .setIfPrimaryTerm(20);
//        try {
//            IndexResponse response = client.index(request1, RequestOptions.DEFAULT);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ElasticsearchException e) {
//            if (e.status() == RestStatus.CONFLICT) {
//                System.out.println("======CONFLICT======");
//            }
//        }
        // Same will happen in case opType was set to create and a document with same index and id already existed:
//        IndexRequest request2 = new IndexRequest("posts")
//                .id("1")
//                .source("field", "value")
//                .opType(DocWriteRequest.OpType.CREATE);
//        try {
//            IndexResponse response = client.index(request2, RequestOptions.DEFAULT);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch(ElasticsearchException e) {
//            if (e.status() == RestStatus.CONFLICT) {
//                System.out.println("======CONFLICT======");
//            }
//        }


        // Asynchronous Executionedit
        //Executing a IndexRequest can also be done in an asynchronous fashion so that the client can
        // return directly. Users need to specify how the response or potential failures will be handled
        // by passing the request and a listener to the asynchronous index method:

        //A typical listener for index looks like:
//        client.indexAsync(indexRequest2, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
//            @Override
//            public void onResponse(IndexResponse indexResponse) {
//                //Called when the execution is successfully completed.
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                //Called when the whole IndexRequest fails.
//            }
//        });
        // The asynchronous method does not block and returns immediately.
        // Once it is completed the ActionListener is called back using the onResponse method if
        // the execution successfully completed or using the onFailure method if it failed.
        // Failure scenarios and expected exceptions are the same as in the synchronous execution case.


        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
