package com.net7.scre.processors;

import com.net7.scre.deduplication.NormalizationField;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;

public class Main {
    public static void main(String args[]) throws ParseException, IOException {

        ArrayList<String> duplicates = new ArrayList<>();
        ScrollQuery sq = new ScrollQuery("deduplication_index");
        sq.connectToElastic();
        duplicates = sq.queryScroll(new Heuristic("oai:doaj.org_article:...", "deduplication_index").buildQuery());
        System.out.println(duplicates);
        sq.closeConnection();

        /*
        HeuristicCycle hc = new HeuristicCycle("deduplication_index");
        hc.findDuplicates();
         */
    }
}
