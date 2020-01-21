package speed_layer.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import utils.utils;
import twitter4j.Status;

// First bolt: filter tweets and calculate their sentiment value
public class ParserBolt extends BaseBasicBolt {

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet","classification"));
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        if(tweet.getId()==0){
            collector.emit(new Values(null,null));
        }
        else {
            /* Tweet is send to next bolt only if it contains query and afinn files dataset
            contain afinn file of the tweet language */
            if (utils.containsLanguageTweet(utils.getLanguageOfTweet(tweet)) && utils.containQuery(tweet.getText())) {
                int sentiment=utils.getSentimentTweet(tweet);
                String classification="Neutral";
                if(sentiment<0)
                    classification="Negative";
                if(sentiment>0)
                    classification="Positive";
                collector.emit(new Values(tweet,classification));
            }
        }
    }
}
