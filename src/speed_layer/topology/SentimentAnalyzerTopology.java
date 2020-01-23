package speed_layer.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import speed_layer.bolts.WriterBolt;
import speed_layer.bolts.ClassifierBolt;
import speed_layer.spouts.TwitterSpout;

public class SentimentAnalyzerTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter_spout", new TwitterSpout(), 1);
        builder.setBolt("parser_bolt", new ClassifierBolt(), 1).
                shuffleGrouping("twitter_spout");
        builder.setBolt("calculator_bolt", new WriterBolt(5), 3).
                fieldsGrouping("parser_bolt", new Fields("classification"));
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        cluster.submitTopology("twitter-analysis-topology", conf, builder.createTopology());
        Thread.sleep(180* 1000);
        cluster.deactivate("twitter-analysis-topology");
        cluster.killTopology("twitter-analysis-topology");
        cluster.shutdown();
    }
}
