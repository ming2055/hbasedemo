package com.elasticsearch.documentAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class UpdateByQueryRequestAPI {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));

        UpdateByQueryRequest request =
                new UpdateByQueryRequest("test");

        request.setConflicts("proceed");
        //Only copy documents which have field user set to kimchy
        request.setQuery(new TermQueryBuilder("user", "kimchy"));
        //Only copy 10 documents
        request.setSize(10);

        //By default UpdateByQueryRequest uses batches of 1000. You can change the batch size with setBatchSize.
        request.setBatchSize(100);

        //Update by query can also use the ingest feature by specifying a pipeline.
//        request.setPipeline("my_pipeline");

        //UpdateByQueryRequest also supports a script that modifies the document:
        //setScript to increment the likes field on all documents with user kimchy
        Map<String, Object> parameters = singletonMap("tag", "blue");
//        request.setScript(
//                new Script(
//                        ScriptType.INLINE, "painless",
//                        "if (ctx._source.user == 'kimchy') {ctx._source.tag=params.tag;}",
//                        parameters));
//        request.setScript(
//                new Script(
//                        ScriptType.INLINE, "painless",
//                        "ctx._source.tags.add(params.tag)",
//                        parameters));

        request.setScript(
                new Script(
                        ScriptType.INLINE, "painless",
                        "ctx._source.tags.add(params.tag)",
                        parameters));
        //UpdateByQueryRequest can be parallelized using sliced-scroll with setSlices:
        request.setSlices(2);
        //UpdateByQueryRequest uses the scroll parameter to control how long it keeps the "search context" alive.
        request.setScroll(TimeValue.timeValueMinutes(10));

        request.setRouting("=cat");

        BulkByScrollResponse bulkResponse =
                client.updateByQuery(request, RequestOptions.DEFAULT);
        client.close();
    }
}
