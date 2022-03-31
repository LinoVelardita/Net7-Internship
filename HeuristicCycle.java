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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;

public class HeuristicCycle {

    private String index_name;
    private RestHighLevelClient client;
    private ArrayList<String> duplicates;

    public HeuristicCycle(String index_name){
        this.index_name = index_name;
        duplicates = new ArrayList<>();
    }

    public void findDuplicates() throws IOException {
        connectToElastic();

        //scroll per prendere uno ad uno gli _id
        SearchRequest request = new SearchRequest(index_name);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(0);
        sourceBuilder.size(500);
        request.source(sourceBuilder);
        request.scroll(TimeValue.timeValueMinutes(15));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        ArrayList<String> results = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            Heuristic h = new Heuristic(client, hit.getId(), index_name,
                    500,
                    0,
                    1,
                    12);
            BoolQueryBuilder find_duplicates = h.buildQuery();
            if(!(find_duplicates == null)){
                singleSearch(find_duplicates, hit);
            }
            results.add(hit.getId());
        }
        System.out.println(duplicates);
        System.out.println("------ results: " + results.size() + ", scrollId: " + response.getScrollId());

        String scrollId = response.getScrollId();

        boolean hasNext = !results.isEmpty();

        while (hasNext) {
            SearchScrollRequest scroll_request = new SearchScrollRequest(scrollId);
            scroll_request.scroll(TimeValue.timeValueMinutes(15));
            SearchResponse searchScrollResponse = client.scroll(scroll_request, RequestOptions.DEFAULT);

            results = new ArrayList<String>();
            for (SearchHit hit : searchScrollResponse.getHits()) {
                Heuristic h = new Heuristic(client, hit.getId(), index_name,
                        500,
                        0,
                        1,
                        12);
                BoolQueryBuilder find_duplicates = h.buildQuery();
                if(!(find_duplicates == null)){
                    singleSearch(find_duplicates, hit);
                }
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

    private void addDuplicate(SearchHit hit, ArrayList<String> ids){
        if(!ids.isEmpty()) System.out.println(hit.getId()+"  ha duplicati:");
        for(String s : ids){
            System.out.println(s);
            if(!duplicates.contains(s)) {
                duplicates.add(s);
            }
        }
    }

    private void connectToElastic(){
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("tirocini", "dpZfBAXLF7qq438T"));
        RestClientBuilder builder = RestClient.builder(new HttpHost("es.tirocini.netseven.it", 443, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        this.client = new RestHighLevelClient(builder);
    }

    private void closeConnection() throws IOException {
        client.close();
    }

    //Query per trovare i duplicati di un singolo documento
    private ArrayList<String> singleSearch(BoolQueryBuilder bq, SearchHit searchHit) throws IOException {
        ArrayList<String> duplicates = new ArrayList<>();
        SearchRequest singleDocRequest = new SearchRequest(index_name);
        SearchSourceBuilder singleDocBuilder = new SearchSourceBuilder();
        singleDocBuilder.query(bq);
        singleDocRequest.source(singleDocBuilder);
        SearchResponse singleDocResponse = client.search(singleDocRequest, RequestOptions.DEFAULT);
        SearchHits results = singleDocResponse.getHits();
        for(SearchHit hit : results){
            duplicates.add(hit.getId());
        }
        if(!duplicates.isEmpty())   duplicates.remove(0);
        if(!duplicates.isEmpty())   System.out.println(searchHit.getId()+" has duplicates:\n"+duplicates);
        return duplicates;
    }

}
