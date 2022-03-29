package com.net7.scre.processors;


import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

public class SimilarQuery {

    RestHighLevelClient client;
    //Map<Doi, Vector<_id>>
    private HashMap<String, Vector<String>> duplicates;


    public SimilarQuery(){
    }

    public SimilarQuery(RestHighLevelClient client){
        this.client = client;
    }

    public void doiQuery(String doi, String index) throws IOException {
        MatchQueryBuilder doi_query = new MatchQueryBuilder("doi.keyword", doi);
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(doi_query);
        sourceBuilder.from(0);
        sourceBuilder.size(10000);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        int n_hit = 0;
        for(SearchHit hit : searchResponse.getHits()){
            System.out.println(hit.toString());
            n_hit++;
        }
        System.out.println("\n-------- n_hit: " + n_hit + " --------");
        System.out.println("\n------ End Doi Query ------\n");
    }

    public void titleQuery(String title, String index) throws IOException {
        ///!!!IMPORTANTE -> aggiungere sempre .keyword
        FuzzyQueryBuilder title_query = new FuzzyQueryBuilder("normalized_title.keyword", title)
                .fuzziness(Fuzziness.ONE)
                .maxExpansions(1000)
                .transpositions(true)
                .prefixLength(0);
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(title_query);
        sourceBuilder.from(0);
        sourceBuilder.size(10000);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        int n_hit = 0;
        for(SearchHit hit : searchResponse.getHits()){
            System.out.println(hit.toString());
            n_hit++;
        }
        System.out.println("\n-------- n_hit: " + n_hit + " --------");
        System.out.println("\n------ End Title Query ------");
    }

    public void numberOfAuthorQuery(int number, String index) throws IOException {
        MatchQueryBuilder num_query = new MatchQueryBuilder("number_of_authors", number);
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(num_query);
        sourceBuilder.from(0);
        sourceBuilder.size(10000);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        int n_hit = 0;
        for(SearchHit hit : searchResponse.getHits()){
            System.out.println(hit.toString());
            n_hit++;
        }
        System.out.println("\n-------- n_hit: " + n_hit + " --------");
        System.out.println("\n------ End number_of_authors Query ------\n");
    }

    public void connectToElastic(){
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("tirocini", "dpZfBAXLF7qq438T"));
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
