package com.net7.scre.deduplication;

import static net.gcardone.junidecode.Junidecode.*;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


import java.util.Iterator;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NormalizationField{

    private String raw_data;
    private String title;
    private String url;
    private Vector<String> identifiers;
    private String Doi;



    public NormalizationField(String raw_data) throws ParseException {
        this.raw_data = new String(raw_data);
        identifiers = new Vector<>();
        Doi = new String();
        title = new String();
        parsing();
    }

    public void addTitle(String title){
        if(title==null) throw new NullPointerException("null title - Normalization.java");
        this.title =  title;
    }

    public String getDoi(){
        return Doi;
    }

    public Vector<String> getIdentifiers(){

        return identifiers;
    }


    /*
    @requires: title != null
    @modifies: title
    @effects: -transform from Unicode to ASCII
              -remove punctuation
              -all letters in lowercase
              -remove blank char at the beginning and at the end of string
    @return: normalized title
    */
    public String getNormalizedTitle() throws NullPointerException{
        if(this.title==null) throw new NullPointerException("null title - Normalization.java");
        String result = unidecode(title);
        result = result.replaceAll("\\p{Punct}", "");
        result = result.toLowerCase();
        result = result.trim();
        return result;
    }

    //must have: "10." + non-null alphanumeric string + '/' + non-null alphanumeric
    /*
    @requires: normalized id string != null
    @modifies: none
    @effects: recognize Doi identifiers
    @return: true -> Doi number else false
     */
    private boolean isDoi(String id) throws NullPointerException{
        if(id==null) throw new NullPointerException();
        Pattern pattern = Pattern.compile("^10.\\p{Alnum}+/+", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(id);
        boolean matchFound = matcher.find();
        return matchFound;
    }

    //must have: 4 digit + "-" + (4 digit or 3 digit and "x")
    private boolean isISSN(String id) throws NullPointerException{
        if(id==null) throw new NullPointerException();
        Pattern pattern = Pattern.compile("[\\d]{4}\\-[\\d]{3}[\\w]", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(id);
        boolean matchFound = matcher.find();
        return matchFound;
    }

    /*
    @requires: id != null
    @modifies: -all letter in lowercase
               -trim
               -remove, if present, the Doi URL
    @effects: normalize identifier
    @return: normalized id
     */
    private String getNormalizedId(String id) throws NullPointerException{
        if(id==null) throw new NullPointerException("null id - getNormalizedId");
        String result = id.toLowerCase();
        result = result.trim();
        result = result.replaceAll("https://doi.org/", "");
        result = result.replaceAll("http://doi.org/", "");  //??necessary??
        result = result.replaceAll("http://dx.doi.org/", "");
        result = result.replaceAll("https://dx.doi.org/", "");
        result = result.replace("\\", "");
        return result;
    }

    //Sometime you can find a doi number inside an URL like this:
    //http://dx.doi.org/10.17026/dans-x45-ryhg
    /*
    @requires: url != null
    @modifies: -
    @effects: recognize Doi number from URL
    @return: Doi number (if presents), or null
     */
    private String getDoiFromUrl(String url){
        if(url == null) throw new NullPointerException("null url - getDoiFromUrl");
        String s = url.replace("\\", "");
        String find = new String("http://dx.doi.org/");
        String find_2 = new String("https://dx.doi.org/");
        boolean check = s.contains(find) || s.contains(find_2);
        if(check) return s.replaceAll(find, "");
        else return null;
    }

    //input -> raw_data string
    public void parsing() throws ParseException {

        JSONParser tmpParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) tmpParser.parse(raw_data);

        for (Iterator iterator = jsonObject.keySet().iterator(); iterator.hasNext();) {
            String key = (String) iterator.next();
            switch (key){
                case "is_based_on_url":
                    Vector<String> urls = new Vector<>();
                    JSONArray urlObject = (JSONArray) jsonObject.get(key);
                    for(Object obj : urlObject){
                        urls.add((String) obj);
                    }
                    for(String s : urls){
                        String doi_number = getDoiFromUrl(s);
                        if(doi_number != null){
                            Doi = doi_number;
                            break;
                        }
                    }
                case "identifier":
                    Vector<String> ids = new Vector<>();
                    JSONArray idObject = (JSONArray) jsonObject.get(key);
                    for(Object obj : idObject){
                        String id_value = (String) obj;
                        id_value = getNormalizedId(id_value);
                        if(isDoi(id_value)) Doi = id_value;
                        else{
                            if(!isISSN(id_value)) ids.add(id_value);
                        }
                    }
                    identifiers.addAll(ids);
            }
        }
    }

}


