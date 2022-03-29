package com.net7.scre.processors;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;

public class ScrollQuery {

    private String index_name;
    private RestHighLevelClient client;

    public ScrollQuery(String index_name){
        this.index_name = index_name;
    }


    public ArrayList<String> queryScroll(QueryBuilder qb) throws IOException {
        ArrayList<String> results = new ArrayList<String>();

        // first request
        SearchRequest request = new SearchRequest(index_name);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(qb);
        sourceBuilder.from(0);
        sourceBuilder.size(1000);
        request.source(sourceBuilder);
        request.scroll(TimeValue.timeValueMinutes(1L));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        for (SearchHit hit : response.getHits()) {
            results.add(hit.getId());
        }

        //System.out.println("------ results: " + results.size() + ", scrollId: " + response.getScrollId());

        // more requests
        String scrollId = response.getScrollId();

        boolean hasNext = !results.isEmpty();

        while (hasNext) {
            SearchScrollRequest scroll_request = new SearchScrollRequest(scrollId);
            scroll_request.scroll(TimeValue.timeValueSeconds(3));
            SearchResponse searchScrollResponse = client.scroll(scroll_request, RequestOptions.DEFAULT);

            scrollId = searchScrollResponse.getScrollId();

            hits = searchScrollResponse.getHits();

            ArrayList<String> newResults = new ArrayList<String>();
            for (SearchHit hit : searchScrollResponse.getHits()) {
                newResults.add(hit.getId());
            }

            results.addAll(newResults);

            //System.out.println("------ results: " + results.size() + ", scrollId: " + searchScrollResponse.getScrollId());
            hasNext = !newResults.isEmpty();
            scrollId = searchScrollResponse.getScrollId();
        }

        results.remove(0);
        return results;
    }

    public void connectToElastic(){
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("???", "???"));
        RestClientBuilder builder = RestClient.builder(new HttpHost("es.tirocini.netseven.it", 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        this.client = new RestHighLevelClient(builder);
    }

    public void closeConnection() throws IOException {
        client.close();
    }




}
