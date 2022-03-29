package com.net7.scre.processors;

import com.net7.scre.utils.QueryBuilder;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.ArrayList;

public class Heuristic {

    private String _id;
    private String index_name;

    private String title;
    private String doi;
    private int n_authors;
    private ArrayList<Integer> year_of_publication;

    public Heuristic(String _id, String index_name) throws IOException {
        this._id = _id;
        this.index_name = index_name;
        fillData();
        fix();
    }

    public Heuristic(String _id, String index_name, String title, String doi, int n_authors, ArrayList year_of_publication){
        this._id = _id;
        this.index_name = index_name;
        this.title = title;
        this.doi = doi;
        this.n_authors = n_authors;
        this.year_of_publication = year_of_publication;
        fix();
    }

    /*
        Equals_doi OR (Equals_n_authors AND SimilarTitle)
     */
    public BoolQueryBuilder buildQuery(){
        //Doi
        MatchQueryBuilder doi_query = new MatchQueryBuilder("doi.keyword", doi);

        //number of authors
        MatchQueryBuilder n_author_query = new MatchQueryBuilder("number_of_authors", n_authors);

        //Title (normalized)
        FuzzyQueryBuilder title_query = new FuzzyQueryBuilder("normalized_title.keyword", title)
                .fuzziness(Fuzziness.ONE)
                .maxExpansions(1000)
                .transpositions(true)
                .prefixLength(0);

        BoolQueryBuilder slice_query = QueryBuilders.boolQuery()
                .must(n_author_query)
                .must(title_query);

        BoolQueryBuilder heuristic = QueryBuilders.boolQuery()
                .should(doi_query)
                .should(slice_query)
                .minimumShouldMatch(1);

        return heuristic;
    }

    private void fillData() throws IOException {
        RestHighLevelClient client = connect();
        GetRequest request = new GetRequest(index_name, _id);
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        title = (String) response.getSourceAsMap().get("normalized_title");
        doi = (String) response.getSourceAsMap().get("doi");
        n_authors = (Integer) response.getSourceAsMap().get("number_of_authors");
        year_of_publication = (ArrayList<Integer>) response.getSourceAsMap().get("year_of_publication");
        client.close();
    }

    private RestHighLevelClient connect(){
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
        return client;
    }

    private void fix(){
        if(doi == null || doi.length()<4) doi = "0";
        if(n_authors == 0) n_authors = -1;
        if(year_of_publication.isEmpty()) year_of_publication.add(0);
        if(title == null || title.length()<2) title = "%%%%%%%%%%";
    }

}
