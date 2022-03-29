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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;

public class HeuristicCycle {

    private String index_name;
    private RestHighLevelClient client;
    private ScrollQuery sq;
    private ArrayList<String> duplicates;

    public HeuristicCycle(String index_name){
        this.index_name = index_name;
        sq = new ScrollQuery(index_name);
        sq.connectToElastic();
        duplicates = new ArrayList<>();
    }

    public void findDuplicates() throws IOException {
        ArrayList<String> results = new ArrayList<>();
        connectToElastic();

        //prima scroll per prendere uno ad uno gli _id
        SearchRequest request = new SearchRequest(index_name);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(0);
        sourceBuilder.size(100);
        request.source(sourceBuilder);
        request.scroll(TimeValue.timeValueMinutes(1L));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        for (SearchHit hit : response.getHits()) {
            //seconda scroll per usare l'euristica
            Heuristic h = new Heuristic(hit.getId(), index_name);
            addDuplicate(sq.queryScroll(h.buildQuery()));
            results.add(hit.getId());
        }
        System.out.println(duplicates);
        System.out.println("------ results: " + results.size() + ", scrollId: " + response.getScrollId());

        String scrollId = response.getScrollId();

        boolean hasNext = !results.isEmpty();

        while (hasNext) {
            SearchScrollRequest scroll_request = new SearchScrollRequest(scrollId);
            scroll_request.scroll(TimeValue.timeValueSeconds(3));
            SearchResponse searchScrollResponse = client.scroll(scroll_request, RequestOptions.DEFAULT);

            scrollId = searchScrollResponse.getScrollId();

            hits = searchScrollResponse.getHits();

            results = new ArrayList<String>();
            for (SearchHit hit : searchScrollResponse.getHits()) {
                Heuristic h = new Heuristic(hit.getId(), index_name);
                addDuplicate(sq.queryScroll(h.buildQuery()));
                results.add(hit.getId());
            }
            System.out.println(duplicates);
            System.out.println("------ results: " + results.size() + ", scrollId: " + searchScrollResponse.getScrollId());
            hasNext = !results.isEmpty();
            scrollId = searchScrollResponse.getScrollId();
        }

        closeConnection();
    }

    public ArrayList<String> getDuplicates(){
        return duplicates;
    }

    private void addDuplicate(ArrayList<String> ids){
        for(String s : ids){
            if(!duplicates.contains(s)) {
                duplicates.add(s);
            }
        }
    }

    private void connectToElastic(){
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

    private void closeConnection() throws IOException {
        client.close();
    }

}
