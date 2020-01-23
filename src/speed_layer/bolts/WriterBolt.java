package speed_layer.bolts;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import utils.utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.base.Stopwatch;
import java.sql.Timestamp;
import java.io.FileWriter;

// Last bolt of the topology: receive classification and increment corresponding value in the hbase table
// Sometimes print result of the speed layer (real time view) in log
public class WriterBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(WriterBolt.class);
    private static final String path_dataset=utils.getValue("PATH_MASTER_DATASET");
    private Table table;

    // Attribute for print real time view in log only occasionally
    private static Stopwatch stopwatch =  Stopwatch.createStarted();;
    private int timeToPrintInLog;

    // Map that contains real time view results
    private static ConcurrentHashMap<String,Integer> sentimentMap=new ConcurrentHashMap<>();

    public WriterBolt(int timeToPrint){
        timeToPrintInLog=timeToPrint;
    }

    @Override
    public final void prepare(final Map map, final TopologyContext topologyContext,
                             final OutputCollector collector) {
        // Access to the query table in hbase. If it not exist create it
        try {
            Configuration configuration = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin  = connection.getAdmin();
            String query=utils.getQuery();

            if(!admin.tableExists(TableName.valueOf(query))) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(query));
                tableDescriptor.addFamily(new HColumnDescriptor("sentiment"));
                admin.createTable(tableDescriptor);
            }
            table = connection.getTable(TableName.valueOf(query));

        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        sentimentMap.put("Positive",0);
        sentimentMap.put("Neutral",0);
        sentimentMap.put("Negative",0);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    // Si usa metodo aggiuntivo per stampare risultati
    @Override
    public void execute(Tuple tuple) {
        Status tweet=(Status) tuple.getValueByField("tweet");
        String classification = (String) tuple.getValueByField("classification");
        if(classification==null && tweet==null){
            printLogCountrySentiment();
        }
        else{
            // Update real time view results
            sentimentMap.put(classification,sentimentMap.get(classification)+1);
            // Reads data from hbase table
            Get g = new Get(Bytes.toBytes(classification));
            try {
                Result resultTable = table.get(g);
                byte[] previousValue = resultTable.getValue(Bytes.toBytes("sentiment"), Bytes.toBytes("value"));

                // Increases data in hbase table
                int result = 1;
                if (Bytes.toString(previousValue) != null && !utils.allZeros(previousValue)) {
                    result =  Integer.parseInt(Bytes.toString(previousValue));
                    result++;
                }
                Put put = new Put(Bytes.toBytes(classification));
                put.addColumn(Bytes.toBytes("sentiment"), Bytes.toBytes("value"), Bytes.toBytes(Integer.toString(result)));
                table.put(put);
                // Send new data to the master dataset for the next batch processing
                updateMasterDataset(tweet);
            }
            catch (IOException e){}
            if (timeToPrintInLog <= stopwatch.elapsed(TimeUnit.SECONDS)) {
                stopwatch.reset();
                stopwatch.start();
                printLogCountrySentiment();
            }
        }
    }
    private void printLogCountrySentiment() {
        final StringBuilder dump = new StringBuilder();
        dump.append("QUERY: "+utils.getQuery()+"\t");
        int positive=sentimentMap.get("Positive");
        int neutral=sentimentMap.get("Neutral");
        int negative=sentimentMap.get("Negative");
        dump.append("\t")
                .append(" ==> Positive: ")
                .append(positive)
                .append(" ==> Neutral: ")
                .append(neutral)
                .append(" ==> Negative: ")
                .append(negative)
                .append("\n");
        logger.info("\n{}", dump.toString());
    }

    /* Methods add new line in master dataset.
        The line contains timestamp so that batch layer processes the new data only starting from the next
        The line contains language so that batch layer can calculate sentiment value by reading the correct afinn file
     */
    private void updateMasterDataset(Status tweet) {
        long timestamp = (new Timestamp(System.currentTimeMillis())).getTime();
        int sentiment=0; // default value
        long id=tweet.getId();
        Date date=tweet.getCreatedAt();
        String flag="NO_QUERY"; // default value
        String user=tweet.getUser().getName();
        String text=tweet.getText().replaceAll("\\n","");;
        String language=tweet.getLang();
        try {
            String[] partsOfTweet=new String[8];
            int positionLangInDataset=Integer.parseInt(utils.getValue("INDEX_LANGUAGE_TWEET_IN_DATASET"));
            int positionTextInDataset=Integer.parseInt(utils.getValue("INDEX_TEXT_TWEET_IN_DATASET"));
            int positionTimestampInDataset=Integer.parseInt(utils.getValue("INDEX_TIMESTAMP_TWEET_IN_DATASET"));
            Queue<String> otherInformation=new LinkedList<>();
            otherInformation.add(Integer.toString(sentiment));
            otherInformation.add(Long.toString(id));
            otherInformation.add(date.toString());
            otherInformation.add(flag);
            otherInformation.add(user);
            if(positionLangInDataset>=0 && positionLangInDataset<=7 &&
                    positionTextInDataset>=0 && positionTextInDataset<=7 &&
                    positionTimestampInDataset>=0 && positionTimestampInDataset<=7) {
                partsOfTweet[positionLangInDataset]=language;
                partsOfTweet[positionTextInDataset]=text;
                partsOfTweet[positionTimestampInDataset]=Long.toString(timestamp);
                StringBuilder stringToAppend = new StringBuilder();
                for(int i=0;i<partsOfTweet.length;i++){
                    if(partsOfTweet[i]==null)
                        partsOfTweet[i]=otherInformation.poll();
                }
                for (int i=0;i< partsOfTweet.length;i++) {
                    stringToAppend.append("\"").append(partsOfTweet[i]);
                    if(i==partsOfTweet.length-1)
                        stringToAppend.append("\"\n");
                    else
                        stringToAppend.append("\",");
                }
                FileWriter pw = new FileWriter(path_dataset, true);
                pw.append(stringToAppend.toString());
                pw.flush();
            }
        }
        catch (IOException e){
        }
        catch (NumberFormatException e){}
    }
}
