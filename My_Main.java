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

public class My_Main {
    public static void main(String args[]) throws ParseException, IOException {

        //SimilarQuery q = new SimilarQuery();
        //q.connectToElastic();
        //q.titleQuery("american studies in belarus", "tony_index");
        //q.doiQuery("10.13136/2281-4582/2021.i17.1001", "tony_index");
        //q.numberOfAuthorQuery(17, "deduplication_index");
        //q.closeConnection();
        //UpdateIndex ui = new UpdateIndex();
        //ui.takeIndex();


        ArrayList<String> duplicates = new ArrayList<>();
        ScrollQuery sq = new ScrollQuery("deduplication_index");
        sq.connectToElastic();
        duplicates = sq.queryScroll(new Heuristic("oai:doaj.org_article:b74030092a4e469e9b3ef78d83d6caed", "deduplication_index").buildQuery());
        System.out.println(duplicates);
        sq.closeConnection();



        /*
        HeuristicCycle hc = new HeuristicCycle("deduplication_index");
        hc.findDuplicates();

         */

        /*
        duplicati:
        oai:doaj.org_article:40eea8a3f78a4ab9b027c13785e2ea9b
        oai:doaj.org_article:53ad6d88a59b44298e9947cb724cfe34
        oai:doaj.org_article:44f0d6b71c274a188de1ab91b3b50012 !!!Doi diverso!!!
         */

    }
}
