package com.elasticsearch.documentAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Map;

public class GetAPI {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));


        GetRequest getRequest = new GetRequest(
                "posts",
                "1");

        //Optional argumentsedit
        //The following arguments can optionally be provided:
        //Disable source retrieval, enabled by default
        getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);

        //Configure source inclusion for specific fields
        String[] includes = new String[]{"message", "*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);


        //Configure source inclusion for specific fields
//        String[] includes = Strings.EMPTY_ARRAY;
//        String[] excludes = new String[]{"message"};
//        FetchSourceContext fetchSourceContext =
//                new FetchSourceContext(true, includes, excludes);
//        getRequest.fetchSourceContext(fetchSourceContext);

        //Configure retrieval for specific stored fields (requires fields to be stored separately in the mappings)
//        getRequest.storedFields("message");
//        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
//        //Retrieve the message stored field (requires the field to be stored separately in the mappings)
//        String message = getResponse.getField("message").getValue();

        // Routing value
        getRequest.routing("routing");

        // Preference value
        getRequest.preference("preference");

        // Set realtime flag to false (true by default)
        getRequest.realtime(false);

        //
        //Perform a refresh before retrieving the document (false by default)
        getRequest.refresh(true);

        // version
        getRequest.version(2);

        //
        getRequest.versionType(VersionType.EXTERNAL);


        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        String index = getResponse.getIndex();
        System.out.println("index: " + index);
        String id = getResponse.getId();
        System.out.println("id: " + id);
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            System.out.println("version: " + version);
            //Retrieve the document as a String
            String sourceAsString = getResponse.getSourceAsString();
            System.out.println("sourceAsString: " + sourceAsString);
            //Retrieve the document as a Map<String, Object>
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            System.out.println("sourceAsMap: " + sourceAsMap);
            //Retrieve the document as a byte[]
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
        } else {
            //Handle the scenario where the document was not found. Note that although the returned response has 404 status code, a valid GetResponse is returned rather than an exception thrown. Such response does not hold any source document and its isExists method returns false.
        }


        GetRequest request = new GetRequest("does_not_exist", "1");
        try {
            GetResponse getResponse1 = client.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                System.err.println("===NOT_FOUND=====");
            }
        }


        try {
            GetRequest request2 = new GetRequest("posts", "1").version(4);
            GetResponse getResponse2 = client.get(request2, RequestOptions.DEFAULT);
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.CONFLICT) {
                System.err.println("===version conflict=====");
            }
        }
//        client.getAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
//            @Override
//            public void onResponse(GetResponse getResponse) {
//
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//
//            }
//        });

        client.close();
    }
}
