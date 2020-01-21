package batch_layer;
import java.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import utils.utils;
import java.sql.Timestamp;

public class TweetSentimentAnalysis extends Configured implements Tool{

    public static class ClassificatorMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static long timestamp=0;
        private static String regex_csv="\",";
        private static int positionText=Integer.parseInt(utils.getValue("INDEX_TEXT_TWEET_IN_DATASET"));
        private static int positionLanguage=Integer.parseInt(utils.getValue("INDEX_LANGUAGE_TWEET_IN_DATASET"));
        private static int positionTimestamp=Integer.parseInt(utils.getValue("INDEX_TIMESTAMP_TWEET_IN_DATASET"));

        private static String default_language=utils.getValue("DEFAULT_LANGUAGE_MASTER_DATASET");
        public static void setTimestamp(long time){
            timestamp=time;
        }

        private String getTweetText(String line){
            String[] parts=line.split(regex_csv);
            String text = parts[positionText];
            return text.replaceAll("\"","");
        }

        private String getTweetLanguage(String line){
            String language="";
            try {
                language = line.split(regex_csv)[positionLanguage];
            }
            catch (ArrayIndexOutOfBoundsException e){
                language=default_language;
            }
            return language.replaceAll("\"","");
        }
        /* Methods returns if a tweet is to analyze in this batch processing or
       if it is already analyzed by speed layer in this computation cycle */
        private boolean tweetIsToAnlayze(String line){
            try {
                long timestampTweet = Long.parseLong(line.split(regex_csv)[positionTimestamp].replaceAll("\"",""));
                return timestampTweet < timestamp;
            }
            catch (ArrayIndexOutOfBoundsException e) {
                return true;
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String lineTweet=value.toString();
            String textTweet=getTweetText(lineTweet);
            String languageTweet=getTweetLanguage(lineTweet);
            if(utils.containsLanguageTweet(languageTweet) && utils.containQuery(textTweet) && tweetIsToAnlayze(lineTweet)) {
                int sentiment;
                String sentimentClass = "Neutral";
                sentiment = utils.getSentiment(textTweet,languageTweet);
                if (sentiment < 0) {
                    sentimentClass = "Negative";
                }
                if (sentiment > 0) {
                    sentimentClass = "Positive";
                }
                context.write(new Text(sentimentClass), new IntWritable(1));
            }
        }
    }


    public static class CalculatorReducer extends TableReducer<Text,IntWritable,Text> {
        private HTable table;
        @Override
        protected void setup(Context context){
            try {
                String query=utils.getValue("QUERY");
                table = new HTable(configuration, query);
            }
            catch (IOException e) {
                System.out.printf("Error: table not found", e);
            }
        }


        @Override
        public void reduce(Text sentimentClass, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int result = 0;
            for (IntWritable value : values) {
                result += value.get();
            }

            // Reads data from the hbase table

            Get g = new Get(Bytes.toBytes(sentimentClass.toString()));
            Result resultTable = table.get(g);
            byte [] previousValue = resultTable.getValue(Bytes.toBytes("sentiment"),Bytes.toBytes("value"));

            // Update value on the hbase table
            if (Bytes.toString(previousValue) != null && !utils.allZeros(previousValue)) {
                int value = Integer.parseInt(Bytes.toString(previousValue));
                result += value;
            }

            System.out.println("REDUCER --> QUERY: "+utils.getQuery()+" --- "+sentimentClass.toString()+" --- "+": "+result );
            Put put = new Put(Bytes.toBytes(sentimentClass.toString()));
            put.addColumn( Bytes.toBytes("sentiment"), Bytes.toBytes("value"), Bytes.toBytes(Integer.toString(result)));
            context.write(sentimentClass, put);
        }
    }

    private static Configuration configuration;

    public int run(String[] args) throws Exception {
        String query = utils.getQuery();
        String dataset_input_file = utils.getValue("PATH_MASTER_DATASET");
        // Access to the query table in hbase. If it not exist create it

        configuration = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(getConf());
        if (!admin.tableExists(TableName.valueOf(query))) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(query));
            tableDescriptor.addFamily(new HColumnDescriptor("sentiment"));
            admin.createTable(tableDescriptor);
        }

        // Initialize table for batch processing
        try {
            HTable hTable = new HTable(configuration, utils.getValue("QUERY"));
            String[] classifications = {"Positive", "Neutral", "Negative"};
            for (String c : classifications) {
                Put put = new Put(Bytes.toBytes(c));
                put.addColumn(Bytes.toBytes("sentiment"), Bytes.toBytes("value"), Bytes.toBytes(0));
                hTable.put(put);
            }
        }
        catch (IOException e){}
        Job job = Job.getInstance(configuration, "Sentiment Analysis");
        job.setJarByClass(TweetSentimentAnalysis.class);
        job.setMapperClass(ClassificatorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(dataset_input_file));
        TableMapReduceUtil.initTableReducerJob(
                query,
                CalculatorReducer.class,
                job);
        job.setReducerClass(CalculatorReducer.class);
        ClassificatorMapper.setTimestamp(new Timestamp(System.currentTimeMillis()).getTime());
        //job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new HBaseConfiguration(), new TweetSentimentAnalysis(), args);
        System.exit(res);
    }
}
