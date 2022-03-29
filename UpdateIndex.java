package com.net7.scre.processors;

import com.net7.scre.deduplication.NormalizationField;
import org.apache.abdera.i18n.rfc4646.enums.Script;
import org.apache.camel.impl.engine.DefaultRuntimeEndpointRegistry;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetFieldMappingsResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.SuggestingErrorOnUnknown;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

public class UpdateIndex {


    public void update() throws IOException, ParseException {
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
        RestHighLevelClient client = new RestHighLevelClient(builder);

        //search

        SearchRequest searchRequest = new SearchRequest("tony_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.from(0);
        sourceBuilder.size(10000);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);



        JSONParser tmpParser = new JSONParser();
        String raw_data = new String();
        String title_triple = new String();
        String tmp = new String();

        int n_hit = 0;

        for(SearchHit hit : searchResponse.getHits()) {
            //parsing every hit
            n_hit++;
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
        System.out.println(n_hit);

        client.close();

    }

}
