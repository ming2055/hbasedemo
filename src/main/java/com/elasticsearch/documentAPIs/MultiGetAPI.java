package com.elasticsearch.documentAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Map;

import static javolution.testing.TestContext.assertNull;

public class MultiGetAPI {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));

        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item(
                "posts",
                "1"));
        request.add(new MultiGetRequest.Item("posts", "2"));

//        String[] includes = new String[] {"foo", "*r"};
//        String[] excludes = Strings.EMPTY_ARRAY;
//        FetchSourceContext fetchSourceContext =
//                new FetchSourceContext(true, includes, excludes);
//        request.add(new MultiGetRequest.Item("posts", "3")
//                .fetchSourceContext(fetchSourceContext));



//        request.add(new MultiGetRequest.Item("posts", "4")
//                .storedFields("foo"));
//        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
//        MultiGetItemResponse item = response.getResponses()[0];
//        String value = item.getResponse().getField("foo").getValue();

        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);

        MultiGetItemResponse firstItem = response.getResponses()[1];
//        assertNull(firstItem.getFailure());
        GetResponse firstGet = firstItem.getResponse();
        String index = firstItem.getIndex();
        String id = firstItem.getId();
        System.out.println(index + "/" + id);
        if (firstGet.isExists()) {
            long version = firstGet.getVersion();
            String sourceAsString = firstGet.getSourceAsString();
            Map<String, Object> sourceAsMap = firstGet.getSourceAsMap();
            byte[] sourceAsBytes = firstGet.getSourceAsBytes();
            System.out.println(sourceAsString);
        } else {

        }

        client.close();
    }
}
