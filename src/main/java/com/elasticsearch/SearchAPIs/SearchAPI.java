package com.elasticsearch.SearchAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SearchAPI {

    public static SearchRequest searchBasic() {
        //1 In its most basic form, we can add a query to the request:
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    public static SearchRequest searchBasicOptionArguments() {
        //2 Let’s first look at some of the optional arguments of a SearchRequest
        SearchRequest searchRequest = new SearchRequest("posts");
        searchRequest.routing("routing");
        //设置IndicesOptions控制如何解析不可用的索引以及如何扩展通配符表达式
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        searchRequest.preference("_local");
        return searchRequest;
    }


    public static SearchRequest searchSourceBuilder() {
        //3 SearchSourceBuilder
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("posts");
        searchRequest.source(sourceBuilder);
        return searchRequest;
    }


    public static SearchRequest searchQueryBuilder() {
        //4. A QueryBuilder can be created using its constructor:
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //Create a full text Match Query that matches the text "kimchy" over the field "user".
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("user", "kimchy");
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        matchQueryBuilder.prefixLength(3);
        matchQueryBuilder.maxExpansions(10);
        //QueryBuilder objects can also be created using the QueryBuilders utility class. This class provides helper methods that can be used to create QueryBuilder objects using a fluent programming style:
//        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("user", "kimchy")
//                .fuzziness(Fuzziness.AUTO)
//                .prefixLength(3)
//                .maxExpansions(10);
        sourceBuilder.query(matchQueryBuilder);
        searchRequest.source(sourceBuilder);
        return searchRequest;
    }


    public static SearchRequest searchSourceSortBuilder() {
        //4. A QueryBuilder can be created using its constructor:
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //The SearchSourceBuilder allows to add one or more SortBuilder instances.
        // There are four special implementations (Field-, Score-, GeoDistance- and ScriptSortBuilder).
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        sourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC));
        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    public static SearchRequest searchSourceFilterBuilder() {
        //4. A QueryBuilder can be created using its constructor:
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //By default, search requests return the contents of the document _source but like in the Rest API you can overwrite this behavior.
        // For example, you can turn off _source retrieval completely:
        sourceBuilder.fetchSource(false);

        //The method also accepts an array of one or more wildcard patterns to control which fields get included or excluded in a more fine grained way:
        String[] includeFields = new String[] {"title", "innerObject.*"};
        String[] excludeFields = new String[] {"user"};
        sourceBuilder.fetchSource(includeFields, excludeFields);
        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    public static SearchRequest highlightSearch(){
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitle =
                new HighlightBuilder.Field("title");
        highlightTitle.highlighterType("unified");
        highlightBuilder.field(highlightTitle);
        HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
        highlightBuilder.field(highlightUser);
        sourceBuilder.highlighter(highlightBuilder);

        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    public static void retrievingHighlighting(SearchHits hits){
        for (SearchHit hit : hits.getHits()) {
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlight = highlightFields.get("user");
            Text[] fragments = highlight.fragments();
            String fragmentString = fragments[0].string();
            System.out.println(fragmentString);
        }
    }


    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("master", 9200, "http"),
                        new HttpHost("master", 9201, "http")));

        SearchRequest searchRequest = highlightSearch();


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
            System.out.println(hit.toString());
        }

        retrievingHighlighting(hits);
        client.close();
    }
}
