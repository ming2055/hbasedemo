package com.elasticsearch.documentAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TermVectorsAPI {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));

        TermVectorsRequest request = new TermVectorsRequest("posts", "1");
        request.setFields("user");




        //optional arguments
        request.setFieldStatistics(false);
        request.setTermStatistics(true);
        request.setPositions(false);
        request.setOffsets(false);
        request.setPayloads(false);

            //Set filterSettings to filter the terms that can be returned based on their tf-idf scores.
        Map<String, Integer> filterSettings = new HashMap<>();
        filterSettings.put("max_num_terms", 3);
        filterSettings.put("min_term_freq", 1);
        filterSettings.put("max_term_freq", 10);
        filterSettings.put("min_doc_freq", 1);
        filterSettings.put("max_doc_freq", 100);
        filterSettings.put("min_word_length", 1);
        filterSettings.put("max_word_length", 10);

        request.setFilterSettings(filterSettings);

        Map<String, String> perFieldAnalyzer = new HashMap<>();
        perFieldAnalyzer.put("user", "keyword");
        request.setPerFieldAnalyzer(perFieldAnalyzer);

        request.setRealtime(false);
        request.setRouting("routing");




        TermVectorsResponse response =
                client.termvectors(request, RequestOptions.DEFAULT);

        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        boolean found = response.getFound();

        System.out.println(found);

        for (TermVectorsResponse.TermVector tv : response.getTermVectorsList()) {
            String fieldname = tv.getFieldName();
            int docCount = tv.getFieldStatistics().getDocCount();
            long sumTotalTermFreq =
                    tv.getFieldStatistics().getSumTotalTermFreq();
            long sumDocFreq = tv.getFieldStatistics().getSumDocFreq();
            if (tv.getTerms() != null) {
                List<TermVectorsResponse.TermVector.Term> terms =
                        tv.getTerms();
                System.out.println("xxxxxxxxxxxxxxxxxxxx");
                for (TermVectorsResponse.TermVector.Term term : terms) {
                    String termStr = term.getTerm();
                    int termFreq = term.getTermFreq();
//                    int docFreq = term.getDocFreq();
//                    long totalTermFreq = term.getTotalTermFreq();
//                    float score = term.getScore();
                    if (term.getTokens() != null) {
                        List<TermVectorsResponse.TermVector.Token> tokens =
                                term.getTokens();
                        for (TermVectorsResponse.TermVector.Token token : tokens) {
                            int position = token.getPosition();
                            int startOffset = token.getStartOffset();
                            int endOffset = token.getEndOffset();
                            String payload = token.getPayload();
                        }
                    }
                }
            }
        }



        client.close();
    }
}
