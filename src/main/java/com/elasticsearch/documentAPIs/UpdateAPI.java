package com.elasticsearch.documentAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class UpdateAPI {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));

        UpdateRequest request = new UpdateRequest(
                "test",
                "1");

        //updates with a scripts
        //Script parameters provided as a Map of objects
        Map<String, Object> parameters = singletonMap("tag", "blue");
        //Create an inline script using the painless language and the previous parameters
//        Script inline = new Script(ScriptType.INLINE, "painless",
//                "ctx._source.counter += params.count", parameters);
//        Script inline = new Script(ScriptType.INLINE, "painless",
//                "ctx._source.name = '111'", parameters);

//        Script inline = new Script(ScriptType.INLINE, "painless",
//                "if (ctx._source.tags.contains(params.tag)) { ctx._source.tags.remove(ctx._source.tags.indexOf(params.tag)) }", parameters);

        //删除某个字段
        Script inline = new Script(ScriptType.INLINE, "painless",
                "ctx._source.remove('name')", parameters);

        //Sets the script to the update request
        request.script(inline);


        //Partial document source provided as a String in JSON format
        UpdateRequest request1 = new UpdateRequest("posts", "1");
        String jsonString = "{" +
                "\"updated\":\"2017-01-01\"," +
                "\"reason\":\"daily update\"" +
                "}";
        request1.doc(jsonString, XContentType.JSON);


        //Partial document source provided as a Map which gets automatically converted to JSON format
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("updated", new Date());
        jsonMap.put("reason", "daily update");
        UpdateRequest request2 = new UpdateRequest("posts", "1")
                .doc(jsonMap);


        //Partial document source provided as an XContentBuilder object, the Elasticsearch built-in helpers to generate JSON content
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.timeField("updated", new Date());
            builder.field("reason", "daily update");
        }
        builder.endObject();
        UpdateRequest request3 = new UpdateRequest("posts", "1")
                .doc(builder);


        //Partial document source provided as Object key-pairs, which gets converted to JSON format
        UpdateRequest request4 = new UpdateRequest("posts", "1")
                .doc("updated", new Date(),
                        "reason", "daily update");

        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"updated"};
        request4.fetchSource(
                new FetchSourceContext(true, includes, excludes));

        //不存在使用 upsert
        //If the document does not already exist, it is possible to define some content that will be inserted as a new document using the upsert method:
        //Similarly to the partial document updates,
        // the content of the upsert document can be defined using methods that accept String,
        // Map, XContentBuilder or Object key-pairs.
//        String jsonString1 = "{\"created\":\"2017-01-01\"}";
//        request4.upsert(jsonString1, XContentType.JSON);


        UpdateResponse updateResponse = client.update(
                request, RequestOptions.DEFAULT);


        String index = updateResponse.getIndex();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {

            System.out.println("------updated------");
        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {

        }


        //When the source retrieval is enabled in the UpdateRequest through the fetchSource method, the response contains the source of the updated document:
        GetResult result = updateResponse.getGetResult();
        if (result.isExists()) {
            String sourceAsString = result.sourceAsString();
            Map<String, Object> sourceAsMap = result.sourceAsMap();
            byte[] sourceAsBytes = result.source();
            System.out.println("---" + sourceAsString);
        } else {

        }


        client.close();
    }
}
