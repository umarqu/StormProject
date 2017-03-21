package storm.starter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Date;
import java.util.Map;

// Created by umarq on 04/12/2014

public class SplitBolt implements IRichBolt {

    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Object temp = tuple.getValue(0);
        Map data = (Map) temp;

        // Example Tweet
        // [RT @toppcrab: sparkling masterpiece @ToppDoggHouse #ToppKlass1stAnnie http://t.co/nj3qQzFFXr]
        // [GAK ADA UANG BISA DP BRO JAKET FAVORIT ANDA @wildan_kk CP. 089693622357/PIN. 2636AE00 http://t.co/3hVWvuLAO2]

        String str = (String) data.get("text");
        String username = (String) data.get("username");
        Date timestamp = (Date) data.get("timestamp");
        long retweetCount = (Long) data.get("retweetCount");

        String[] words = str.trim().split("\\s+");

        for (String word : words) {

            //separate and emit hashtags
            if (word.startsWith("#")) {
                String hashtag = word.substring(1, word.length()).toUpperCase();
                _collector.emit("hashtags", new Values(hashtag, timestamp));
            }

            //separate and emit mentions
            if (word.startsWith("@")) {
                String mention = word.substring(1, word.length());
                _collector.emit("mentions", new Values(mention, timestamp));
                _collector.emit("links", new Values(username, mention));
            }
        }

        _collector.emit("usernames", new Values(username, timestamp));
    }

    @Override
    public void cleanup() {

    }

    @Override
    //OUTPUT
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("usernames", new Fields("username", "timestamp"));
        declarer.declareStream("hashtags", new Fields("hashtag", "timestamp"));
        declarer.declareStream("mentions", new Fields("mention", "timestamp"));
        declarer.declareStream("links", new Fields("username", "mention"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
