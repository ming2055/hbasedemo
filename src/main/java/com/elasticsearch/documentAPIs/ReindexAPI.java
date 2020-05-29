package com.elasticsearch.documentAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;

import java.io.IOException;

/**
 * A ReindexRequest can be used to copy documents from one or more indexes into a destination index.
 */
public class ReindexAPI {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));


        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices("test", "posts");
        request.setDestIndex("dest");

        request.setDestVersionType(VersionType.EXTERNAL);
        //By default version conflicts abort the _reindex process but you can just count them instead with:
        request.setConflicts("proceed");

        BulkByScrollResponse bulkResponse =
                client.reindex(request, RequestOptions.DEFAULT);


        client.close();
    }
}
