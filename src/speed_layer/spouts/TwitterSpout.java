package speed_layer.spouts;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import utils.utils;
import twitter4j.*;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

// Spout: open twitter stream, load tweets and send them to first bolt of the topology
public class TwitterSpout extends BaseRichSpout {
    private TwitterStream twitterStream;
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> tweets;

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.tweets=new LinkedBlockingQueue<>();
        this.collector=collector;
        this.twitterStream= utils.openTwitterStream();
        final StatusListener statusListener = new StatusListener() {

            @Override
            public void onStatus(final Status status) {
                tweets.offer(status);
            }

            @Override
            public void onDeletionNotice(final StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(final int i) {
            }

            @Override
            public void onScrubGeo(final long l, final long l1) {
            }

            @Override
            public void onStallWarning(final StallWarning stallWarning) {
            }

            @Override
            public void onException(final Exception e) {
            }
        };
        this.twitterStream.addListener(statusListener);
        FilterQuery filter=new FilterQuery();
        String[] query={utils.getQuery()};
        filter.track(query);
        this.twitterStream.filter(filter);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

public void nextTuple(){
    Status status = tweets.poll();
    if(status!=null) {
        this.collector.emit(new Values(status));
    }
    else{
        try {
            Thread.sleep(500);
        }
        catch (InterruptedException e){}
    }
}

    /* When spout is deactivated it sends a special tuple for notification to the last bolt
      that it must print in log for the last time */
    @Override
    public final void deactivate(){
        this.collector.emit(new Values(new Status() {
            @Override
            public Date getCreatedAt() {
                return null;
            }

            @Override
            public long getId() {
                return 0;
            }

            @Override
            public String getText() {
                return null;
            }

            @Override
            public String getSource() {
                return null;
            }

            @Override
            public boolean isTruncated() {
                return false;
            }

            @Override
            public long getInReplyToStatusId() {
                return 0;
            }

            @Override
            public long getInReplyToUserId() {
                return 0;
            }

            @Override
            public String getInReplyToScreenName() {
                return null;
            }

            @Override
            public GeoLocation getGeoLocation() {
                return null;
            }

            @Override
            public Place getPlace() {
                return null;
            }

            @Override
            public boolean isFavorited() {
                return false;
            }

            @Override
            public boolean isRetweeted() {
                return false;
            }

            @Override
            public int getFavoriteCount() {
                return 0;
            }

            @Override
            public User getUser() {
                return null;
            }

            @Override
            public boolean isRetweet() {
                return false;
            }

            @Override
            public Status getRetweetedStatus() {
                return null;
            }

            @Override
            public long[] getContributors() {
                return new long[0];
            }

            @Override
            public int getRetweetCount() {
                return 0;
            }

            @Override
            public boolean isRetweetedByMe() {
                return false;
            }

            @Override
            public long getCurrentUserRetweetId() {
                return 0;
            }

            @Override
            public boolean isPossiblySensitive() {
                return false;
            }

            @Override
            public String getLang() {
                return null;
            }

            @Override
            public Scopes getScopes() {
                return null;
            }

            @Override
            public String[] getWithheldInCountries() {
                return new String[0];
            }

            @Override
            public int compareTo(Status o) {
                return 0;
            }

            @Override
            public UserMentionEntity[] getUserMentionEntities() {
                return new UserMentionEntity[0];
            }

            @Override
            public URLEntity[] getURLEntities() {
                return new URLEntity[0];
            }

            @Override
            public HashtagEntity[] getHashtagEntities() {
                return new HashtagEntity[0];
            }

            @Override
            public MediaEntity[] getMediaEntities() {
                return new MediaEntity[0];
            }

            @Override
            public ExtendedMediaEntity[] getExtendedMediaEntities() {
                return new ExtendedMediaEntity[0];
            }

            @Override
            public SymbolEntity[] getSymbolEntities() {
                return new SymbolEntity[0];
            }

            @Override
            public RateLimitStatus getRateLimitStatus() {
                return null;
            }

            @Override
            public int getAccessLevel() {
                return 0;
            }
        }));
    }

    @Override
    public final void close() {
        this.twitterStream.cleanUp();
        this.twitterStream.shutdown();
    }
}
