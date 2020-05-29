package com.elasticsearch.documentAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class DeleteAPI {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));

        DeleteRequest request = new DeleteRequest(
                "posts",
                "1");



        //Routing value
        request.routing("routing");
        //Timeout to wait for primary shard to become available as a TimeValue
        request.timeout(TimeValue.timeValueMinutes(2));
        //Timeout to wait for primary shard to become available as a String
        request.timeout("2m");
        //Refresh policy as a WriteRequest.RefreshPolicy instance
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        //Refresh policy as a String
        request.setRefreshPolicy("wait_for");
        //Version  需要高于目前得版本 否则会报版本冲突
        request.version(9);
        //Version type
        request.versionType(VersionType.EXTERNAL);




        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
        String index = deleteResponse.getIndex();
        String id = deleteResponse.getId();
        long version = deleteResponse.getVersion();
        ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            //Handle the situation where number of successful shards is less than total shards
            System.out.println("Handle the situation where number of successful shards is less than total shards");
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure :
                    shardInfo.getFailures()) {
                String reason = failure.reason();
                //Handle the potential failures
                System.out.println(reason);
            }
        }


//        try {
//            DeleteResponse deleteResponse = client.delete(
//                    new DeleteRequest("posts", "1").setIfSeqNo(9).setIfPrimaryTerm(3),
//                    RequestOptions.DEFAULT);
//        } catch (ElasticsearchException exception) {
//            if (exception.status() == RestStatus.CONFLICT) {
//                System.out.println("there is a version conflict");
//            }
//        }

        client.close();
    }
}
