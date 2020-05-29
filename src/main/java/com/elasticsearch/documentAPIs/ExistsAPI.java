package com.elasticsearch.documentAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

/**
 * The exists API returns true if a document exists, and false otherwise.
 */
public class ExistsAPI {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));

        GetRequest getRequest = new GetRequest(
                "posts",
                "1");
        //Disable fetching _source.
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        //Disable fetching stored fields.
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);

        System.out.println(exists);

        client.close();
    }
}
