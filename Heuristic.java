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

//Dato l'id di un documento e il suo indice, costruisce la query
public class Heuristic {

    private String _id;
    private String index_name;

    private String title;   //titolo normalizzato
    private String doi;
    private int n_authors;  //numero di autori
    private ArrayList<String> year_of_publication; //anni di pubblicazione (possono essere più di uno)

    //valori soglia(threshold) per costruire l'euritsica
    private int title_threshold;    //max_expansions parameter for fuzzy_search
    private int n_authors_threshold;    //0 -> esatto numero di autori
                                        //1 -> numero di autori +-1
    private int years_threshold;
    private int title_size_threshold;   //lunghezza minima per poter inserire il titolo nell'euristica

    public Heuristic(String _id, String index_name, int title_threshold, int n_authors_threshold, int years_threshold, int title_size_threshold) throws IOException {
        this._id = _id;
        this.index_name = index_name;

        //threshold to build Heuristic Query
        this.title_threshold = title_threshold;
        this.n_authors_threshold = n_authors_threshold;
        this.years_threshold = years_threshold;
        this.title_size_threshold = title_size_threshold;
        fillData();
    }

    public Heuristic(String _id, String index_name, String title, String doi, int n_authors, ArrayList<String> year_of_publication){
        this._id = _id;
        this.index_name = index_name;
        this.title = title;
        this.doi = doi;
        this.n_authors = n_authors;
        this.year_of_publication = year_of_publication;
    }

    /*
        ritorna null se non ho abbastanza dati per poter usare l'euristica
        else ritorna una BoolQuery
     */
    public BoolQueryBuilder buildQuery(){
        //variabile che incremento ogni volta che ho dati significativi per poter lanciare una query
        //eg. se ho il Doi -> should++,
        //se ho titolo e almeno uno tra n_authors e year_of_pubblication -> should++
        //per poter lanciare l'euristica su un certo documento, il suo should deve essere >=1
        boolean exists_doi = false;
        boolean exists_data = false;


        if(doi!=null && !doi.equals("")) exists_doi=true;
        if(title.length() > title_size_threshold &&
                (!year_of_publication.isEmpty() || n_authors!=0)) exists_data=true;
        //se non ho ne doi ne abbastanza dati -> niente query
        if(!exists_doi && !exists_data){
            System.out.println("Poor data for: "+_id);
            return null;
        }


        BoolQueryBuilder heuristic = QueryBuilders.boolQuery();
        BoolQueryBuilder sliceQuery = QueryBuilders.boolQuery();

        if(exists_doi)
            heuristic.should(new MatchQueryBuilder("doi.keyword", doi));

        if(exists_data){
            sliceQuery.must(BuildTitleQuery());
            BoolQueryBuilder bq_year = BuildYearQuery();
            BoolQueryBuilder bq_n_authors = BuildNAuthorsQuery();
            if(bq_year != null)
                sliceQuery.should(bq_year);
            if(bq_n_authors != null)
                sliceQuery.should(bq_n_authors);
            sliceQuery.minimumShouldMatch(1);

            heuristic.should(sliceQuery);
        }
        /*
            minimumShouldMatch(1) ->
            se trovo il doi uguale
            oppure se trovo dati simili (con i threshold)
         */
        heuristic.minimumShouldMatch(1);

        return heuristic;
    }

    private void fillData() throws IOException {
        RestHighLevelClient client = connect();         //CONNECT TO ELASTIC
        GetRequest request = new GetRequest(index_name, _id);
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        title = (String) response.getSourceAsMap().get("normalized_title");
        doi = (String) response.getSourceAsMap().get("doi");
        n_authors = (Integer) response.getSourceAsMap().get("number_of_authors");
        year_of_publication = (ArrayList<String>) response.getSourceAsMap().get("year_of_publication");
        client.close();             //CLOSE CONNECTION
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



    //------------Query Builders di supporto-------------------

    //Costruisce la fuzzy Query sul titolo normalizzato
    private FuzzyQueryBuilder BuildTitleQuery(){

        FuzzyQueryBuilder title_query = new FuzzyQueryBuilder("normalized_title.keyword", title)
                .fuzziness(Fuzziness.ONE)
                .maxExpansions(title_threshold) //soglia per la somiglianza del titolo
                .transpositions(true)
                .prefixLength(0);

        return title_query;
    }

    private BoolQueryBuilder BuildYearQuery(){

        if(year_of_publication.size()==0) return null;  //Non ho il dato

        BoolQueryBuilder year_pubblication_query = QueryBuilders.boolQuery();
        for(String year : year_of_publication){
            year_pubblication_query.should(new MatchQueryBuilder("year_of_publication.keyword", year));
        }
        year_pubblication_query.minimumShouldMatch(years_threshold);
        return year_pubblication_query;
    }


    private BoolQueryBuilder BuildNAuthorsQuery(){

        if(n_authors == 0) return null; //Non ho il dato

        BoolQueryBuilder n_authors_query = QueryBuilders.boolQuery()
                .should(new MatchQueryBuilder("number_of_authors", n_authors));
        if(n_authors_threshold == 1){
            n_authors_query.should(new MatchQueryBuilder("number_of_authors", n_authors+1));
            n_authors_query.should(new MatchQueryBuilder("number_of_authors", n_authors-1));
        }
        n_authors_query.minimumShouldMatch(1);
        return n_authors_query;
    }

}
