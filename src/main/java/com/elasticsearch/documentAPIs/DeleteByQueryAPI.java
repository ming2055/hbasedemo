package com.elasticsearch.documentAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.io.IOException;

public class DeleteByQueryAPI {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));

        DeleteByQueryRequest request =
                new DeleteByQueryRequest("posts", "test");

        request.setConflicts("proceed");

        request.setQuery(new TermQueryBuilder("user", "kimchy"));

        request.setSize(10);

        request.setBatchSize(100);

        request.setSlices(2);

        request.setScroll(TimeValue.timeValueMinutes(10));

        request.setRouting("=cat");

        BulkByScrollResponse bulkResponse =
                client.deleteByQuery(request, RequestOptions.DEFAULT);

        client.close();
    }
}
