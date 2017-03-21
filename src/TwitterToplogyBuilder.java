package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.spout.TwitterSpout;

// Created by gperinazzo on 28/11/2014

public class TwitterToplogyBuilder {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //Spout
        builder.setSpout("spout", new TwitterSpout(), 1);

        //Pre-processing bolt
        builder.setBolt("split", new SplitBolt(), 9).shuffleGrouping("spout");

        /*20 seconds processing bolt chains*/

        // Most active user
        builder.setBolt("user-counter", new RollingCountBolt(9, 3), 3).fieldsGrouping("split", "usernames", new Fields("username"));
        builder.setBolt("user-intermediate-ranking", new IntermediateRankingsBolt(10), 3).fieldsGrouping("user-counter", new Fields("obj"));
        builder.setBolt("user-total-ranking", new TotalRankingsBolt(10)).globalGrouping("user-intermediate-ranking");
        builder.setBolt("user-ranking-print", new RankingPrinterBolt("USER_RANKING.txt")).shuffleGrouping("user-total-ranking");

        // Mentions
        builder.setBolt("mentions-counter", new RollingCountBolt(9, 3), 3).fieldsGrouping("split", "mentions", new Fields("mention"));
        builder.setBolt("mentions-intermediate-ranking", new IntermediateRankingsBolt(10), 3).fieldsGrouping("mentions-counter", new Fields("obj"));
        builder.setBolt("mentions-total-ranking", new TotalRankingsBolt(10)).globalGrouping("mentions-intermediate-ranking");
        builder.setBolt("mentions-ranking-print", new RankingPrinterBolt("MENTIONS_RANKING.txt")).shuffleGrouping("mentions-total-ranking");

        // Community Graph
        builder.setBolt("graph-connections", new GraphBolt()).fieldsGrouping("split", "links", new Fields("mention"));



        /*6 hour Hashtag Processing bolt chains*/

        // Hashtags 5 minute sliding window
        builder.setBolt("hashtag-counter", new RollingCountBolt(300, 60), 3).fieldsGrouping("split", "hashtags", new Fields("hashtag"));
        builder.setBolt("hashtag-intermediate-ranking", new IntermediateRankingsBolt(10), 3).fieldsGrouping("hashtag-counter", new Fields("obj"));
        builder.setBolt("hashtag-total-ranking", new TotalRankingsBolt(5)).globalGrouping("hashtag-intermediate-ranking");
        builder.setBolt("hashtag-ranking-print", new CSVPrinterBolt("hashtags.csv", 5)).shuffleGrouping("hashtag-total-ranking");

        // Hashtags 10 minute sliding window
        builder.setBolt("hashtag-counter-10", new RollingCountBolt(600, 60), 3).fieldsGrouping("split", "hashtags", new Fields("hashtag"));
        builder.setBolt("hashtag-intermediate-ranking-10", new IntermediateRankingsBolt(10), 3).fieldsGrouping("hashtag-counter-10", new Fields("obj"));
        builder.setBolt("hashtag-total-ranking-10", new TotalRankingsBolt(5)).globalGrouping("hashtag-intermediate-ranking-10");
        builder.setBolt("hashtag-ranking12-print-10", new CSVPrinterBolt("hashtags-10.csv", 10)).shuffleGrouping("hashtag-total-ranking-10");

        // Hashtags 20 minute sliding window
        builder.setBolt("hashtag-counter-20", new RollingCountBolt(1200, 60), 3).fieldsGrouping("split", "hashtags", new Fields("hashtag"));
        builder.setBolt("hashtag-intermediate-ranking-20", new IntermediateRankingsBolt(10), 3).fieldsGrouping("hashtag-counter-20", new Fields("obj"));
        builder.setBolt("hashtag-total-ranking-20", new TotalRankingsBolt(5)).globalGrouping("hashtag-intermediate-ranking-20");
        builder.setBolt("hashtag-ranking-print-20", new CSVPrinterBolt("hashtags-20.csv", 20)).shuffleGrouping("hashtag-total-ranking-20");



        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            StormTopology topology = builder.createTopology();

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, topology);

            builder.createTopology();

            try {
                //kills topology after 6 hours
                Thread.sleep(21600000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            cluster.shutdown();
        }
    }
}