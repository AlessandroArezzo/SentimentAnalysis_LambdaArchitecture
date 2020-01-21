package utils;

import javafx.util.Pair;
import twitter4j.auth.AccessToken;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


// Class contains dataset, methods to access the dataset and other useful methods
public class utils {

    // Url properties file
    private final static String propertiesPath="twitter_config.properties";

    // Query on which to perform sentiment analysis
    private final static String QUERY=getValue("QUERY");
    public static String getQuery(){ return QUERY; }

    /* List of countries memorized in the file countryInfo.txt
     */
    private static List<String> countries=loadCountriesToAnalyze();

    /* Dataset: they associate to each country (expressed with name in English) its name in native language,
     its official language
     */
    private static HashMap<String, String> countries_names = loadCountriesNames();
    private static HashMap<String, String> countries_languages = loadCountriesLanguages();

    /* Dataset: for each language (expressed in alpha_2 code) it associate he respective afinn dictionary
    Each afinn dictionary associates to each word contained in the afinn file the respective sentiment value
     */
    private static HashMap<String,HashMap<String,Integer>> afinnByLanguages=loadAfinnFiles();

    //Method returns the path of a file by reading its property specified in the configuration file
    public static String getValue(String property){
        String value = "";
        try (InputStream input = new FileInputStream(propertiesPath)) {
            Properties prop = new Properties();
            prop.load(input);
            value = prop.getProperty(property);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return value;
    }

    //Methods load data from dataset country info and afinn files (store as static class attributes)
    private static List<String> loadCountriesToAnalyze(){
        List<String> countries=new LinkedList<>();
        String filePath = getValue("PATH_DICTIONARY_COUNTRIES_FILE");
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (!parts[0].equals("#")) {
                    String country = parts[0].trim();
                    countries.add(country);
                }
            }
        }
        catch (FileNotFoundException e) { }
        catch (java.io.IOException e) { }
        return countries;
    }

    private static HashMap<String,String> loadCountriesNames() {
        String filePath = getValue("PATH_DICTIONARY_COUNTRIES_FILE");
        HashMap<String, String> countries_names = new HashMap<>();
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (!parts[0].equals("#")) {
                    String name_en = parts[0].trim();
                    String name_nat = parts[1].trim();
                    if (countries.contains(name_en) || countries.contains((name_nat))) {
                        countries_names.put(name_en, name_nat);
                    }
                }
            }
        } catch (FileNotFoundException e) {
        } catch (java.io.IOException e) {
        }
        for (String country : countries) {
            if (!countries_names.keySet().contains(country) && !countries_names.values().contains(country)) {
                countries.remove(country);
            }
        }
        return countries_names;
    }
    private static HashMap<String,String> loadCountriesLanguages() {
        String filePath = getValue("PATH_DICTIONARY_COUNTRIES_FILE");
        HashMap<String, String> countries_languages= new HashMap<>();
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (!parts[0].equals("#")) {
                    String name_en = parts[0].trim();
                    String lang = parts[2].trim();
                    String name_nat = parts[1].trim();
                    if (countries.contains(name_en) || countries.contains((name_nat))) {
                        countries_languages.put(name_en, lang);
                    }
                }
            }
        } catch (FileNotFoundException e) {
        } catch (java.io.IOException e) {
        }
        return countries_languages;
    }
    private static HashMap<String,HashMap<String,Integer>> loadAfinnFiles() {
        String filePath =getValue("PATH_AFINN_FILES");
        HashMap<String,HashMap<String,Integer>> afinnByLanguages=new HashMap<>();
        ArrayList<String> allLanguagesToAnalyze=new ArrayList<>((countries_languages.keySet().stream().collect(Collectors.groupingBy(k -> countries_languages.get(k)))).keySet());
        for (String language : allLanguagesToAnalyze) {
            String path = filePath;
            afinnByLanguages.put(language, new HashMap<String, Integer>());
            path = path + "AFINN-165-"+language + ".txt";
            String line;
            try {
                BufferedReader reader = new BufferedReader(new FileReader(path));
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t", 2);
                    afinnByLanguages.get(language).put(parts[0].toLowerCase(), Integer.parseInt(parts[1]));
                }
            } catch (FileNotFoundException e) {
            } catch (java.io.IOException e) {
            }
        }
        return afinnByLanguages;
    }

    /*
    private static HashMap<String, double[][]> countries_locations = loadCountriesLocations();
    private static HashMap<String,double[][]> loadCountriesLocations() {
        String filePath = getValue("PATH_DICTIONARY_COUNTRIES_FILE");
        HashMap<String, double[][]> countries_locations= new HashMap<>();
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (!parts[0].equals("#")) {
                    String name_en = parts[0].trim();
                    String[] parts_loc = parts[3].split("[(,)]");
                    String name_nat = parts[1].trim();
                    if (countries.contains(name_en) || countries.contains((name_nat))) {
                        double[][] location = {{Double.parseDouble(parts_loc[1]), Double.parseDouble(parts_loc[2])},
                                {Double.parseDouble(parts_loc[3]), Double.parseDouble(parts_loc[4])}};
                        countries_locations.put(name_en, location);
                    }
                }
            }
        } catch (FileNotFoundException e) {
        } catch (java.io.IOException e) {
        }
        return countries_locations;
    }

     */

    //Method returns language associated with the country
    public static String getLanguageByCountry(String country){
        return countries_languages.get(country);
    }


    // Method returns if the language has the respective afinn file
    public static boolean containsLanguageTweet(String language){
        if(language!=null) {
            return countries_languages.values().contains(language);
        }
        return false;
    }

    // Method returns English name of the country of the tweet. If country is not in the list to be analyzed return null
    public static String getNameCountry(Status tweet){
        if(tweet.getPlace()!=null) {
            String tweet_country = tweet.getPlace().getCountry();
            if (countries_names.containsKey(tweet_country)) {
                return tweet_country;
            }
            for (String country : countries_names.keySet()) {
                if (tweet_country.equals(countries_names.get(country))) {
                    return country;
                }
            }
        }
        return null;
    }

    // Method returns language of the tweet. If language is undetermined by default returns the country language of the tweet
    public static String getLanguageOfTweet(Status tweet){
        String tweet_language=tweet.getLang();
        if(tweet_language.equals("und")){
            String tweet_country=getNameCountry(tweet);
            tweet_language=getLanguageByCountry(tweet_country);
        }
        return tweet_language;
    }

    // Method returns sentiment value of a tweet
    public static int getSentimentTweet(Status tweet){
        String text=tweet.getText().toLowerCase();
        String tweet_language=getLanguageOfTweet(tweet);
        return getSentiment(text,tweet_language);
        //System.out.println("("+tweet_language+"): "+tweet.getText()+" -->Calculating sentiment...");
    }

    // Method returns sentiment value of a String in a language
    public static int getSentiment(String text, String tweet_language){
        String[] words = text.split(" ");
        int sentiment=0;
        for(int i=0;i<words.length;i++){
            String stringToCalculate=null;
            String singleWord=words[i];
            String composed="";
            if(i<words.length-1) {
                composed = words[i] + " " + words[i + 1];
            }
            if(afinnByLanguages.get(tweet_language).containsKey(composed)) {
                stringToCalculate=composed;
                i++;
            }
            else if(afinnByLanguages.get(tweet_language).containsKey(singleWord)){
                stringToCalculate=singleWord;
            }
            if (stringToCalculate!=null) {
                try {
                    //System.out.println("Word: " + stringToCalculate + " --> Score: " + afinnByLanguages.get(tweet_language).get(stringToCalculate));
                    sentiment += afinnByLanguages.get(tweet_language).get(stringToCalculate);
                }
                catch (NullPointerException e){
                    System.out.println("Error");
                }
            }
        }
        //System.out.println("Value sentiment of tweet is "+sentiment+"\n");
        return sentiment;
    }

    // Method return if a String contains the query
    public static boolean containQuery(String text){
        String[] words=text.split(" ");
        String tagQuery="@"+QUERY;
        String hashtagQuery="#"+QUERY;
        String lowerQuery=QUERY.toLowerCase();
        String upperQuery=QUERY.toUpperCase();
        for (String word: words){
            if(word.equals(QUERY) || word.equals(tagQuery) || word.equals(hashtagQuery)
                    || word.equals(lowerQuery) || word.equals(upperQuery))
                return true;
        }
        return false;
    }

    // Methods open the twitter stream reading the app keys from the configuration file
    public static TwitterStream openTwitterStream() {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setIncludeEntitiesEnabled(true);
        String OAUTH_CONSUMER_KEY = getValue("OAUTH_CONSUMER_KEY");
        String OAUTH_CONSUMER_SECRET = getValue("OAUTH_CONSUMER_SECRET");
        String OAUTH_ACCESS_TOKEN = getValue("OAUTH_ACCESS_TOKEN");
        String OAUTH_ACCESS_TOKEN_SECRET = getValue("OAUTH_ACCESS_TOKEN_SECRET");
        configurationBuilder.setOAuthConsumerKey(OAUTH_CONSUMER_KEY);
        configurationBuilder.setOAuthConsumerSecret(OAUTH_CONSUMER_SECRET);
        configurationBuilder.setOAuthAccessToken(OAUTH_ACCESS_TOKEN);
        configurationBuilder.setOAuthAccessTokenSecret(OAUTH_ACCESS_TOKEN_SECRET);
        return new TwitterStreamFactory(configurationBuilder.build()).getInstance();
    }

    // Methods return if a byte[] contains only 0
    public static boolean allZeros( byte[] bytes ){
        for( int i = 0; i < bytes.length; ++i ){
            if( bytes[i] != 0 )
                return false;
        }
        return true;
    }


}