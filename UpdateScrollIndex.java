package com.net7.scre.processors;

import com.net7.scre.deduplication.NormalizationField;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class UpdateScrollIndex {

    private String index_name;
    private String raw_data;
    private String title_triple;
    private String tmp;
    private JSONParser tmpParser;
    private RestHighLevelClient client;

    public UpdateScrollIndex(String index_name){
        this.index_name = index_name;
        raw_data = new String();
        title_triple = new String();
        tmp = new String();
        tmpParser = new JSONParser();
    }

    public void updateScroll() throws IOException, ParseException {
        //connect to elastic
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

        //Search Request to retrieve doc metadata
        SearchRequest searchRequest = new SearchRequest(this.index_name);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.from(0);
        sourceBuilder.size(100);
        searchRequest.source(sourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //JSON Parser
        JSONParser tmpParser = new JSONParser();
        String raw_data = new String();
        String title_triple = new String();
        String tmp = new String();

        SearchHits hits = searchResponse.getHits();
        int hit_count = 0;
        for (SearchHit hit : searchResponse.getHits()) {
            hit_count++;
            update(hit);
        }

        //more request
        String scrollId = searchResponse.getScrollId();
        System.out.println("scrollId: "+scrollId);
        while (hit_count != 0) {
            SearchScrollRequest scroll_request = new SearchScrollRequest(scrollId);
            scroll_request.scroll(TimeValue.timeValueSeconds(3600));
            SearchResponse searchScrollResponse = client.scroll(scroll_request, RequestOptions.DEFAULT);

            scrollId = searchScrollResponse.getScrollId();

            hits = searchScrollResponse.getHits();

            hit_count = 0;
            for (SearchHit hit : searchScrollResponse.getHits()) {
                hit_count++;
                update(hit);
            }
            scrollId = searchScrollResponse.getScrollId();
            System.out.println("scrollId: "+scrollId);
        }

        client.close();

    }

    private void update(SearchHit hit) throws ParseException, IOException {

        tmp = hit.toString();
        JSONObject jsonObject = (JSONObject) tmpParser.parse(tmp);
        JSONObject jsonObject1 = (JSONObject) jsonObject.get("_source");

        //parsing title
        JSONArray titleArray = (JSONArray) jsonObject1.get("title_triple");
        JSONObject titleObj = null;
        if(!titleArray.isEmpty()) {
            titleObj = (JSONObject) titleArray.get(0);
            title_triple = (String) titleObj.get("text");
        }
        else{
            title_triple = "";
        }

        //parsing raw_data and add title
        raw_data = (String) jsonObject1.get("raw_data");
        NormalizationField nf = new NormalizationField(raw_data);
        nf.addTitle(title_triple);

        UpdateRequest request = new UpdateRequest(
                hit.getIndex(), hit.getId());
        Map<String, Object> updating = singletonMap("doi", nf.getDoi());
        Map<String, Object> updating2 = singletonMap("identifiers", nf.getIdentifiers());
        Map<String, Object> updating3 = singletonMap("normalized_title", nf.getNormalizedTitle());

        request.doc(updating);
        client.update(request, RequestOptions.DEFAULT);
        request.doc(updating2);
        client.update(request, RequestOptions.DEFAULT);
        request.doc(updating3);
        client.update(request, RequestOptions.DEFAULT);
    }

}
