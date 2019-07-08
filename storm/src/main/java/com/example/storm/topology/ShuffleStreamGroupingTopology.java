package com.example.storm.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.example.storm.bolt.ConsolePrinterBolt;
import com.example.storm.spout.SentenceSpout;
import com.example.storm.spout.SentenceSpoutWithStreamId;

/**
 * Created by rahul on 3/2/15.
 */
public class ShuffleStreamGroupingTopology {

    public static void main(String args[]) throws AlreadyAliveException, InvalidTopologyException {
        if(args.length != 2) {
            throw new IllegalArgumentException("Topology name and Second argument true/false required. true means ShuffleGrouping with stream id will be used. false means ShuffleGrouping with default stream will be used.");
        }

        boolean streamId = Boolean.valueOf(args[1]);

        StormTopology stormTopology = null;
        if(streamId) {
            stormTopology = ShuffleStreamGroupingWithStreamIdTopology.createTopology();
        } else {
            stormTopology = ShuffleStreamGroupingWithOutStreamIdTopology.createTopology();
        }

        Config config = new Config();
        config.setDebug(false);

        StormSubmitter.submitTopology(args[0], config, stormTopology);
    }

    public static class ShuffleStreamGroupingWithOutStreamIdTopology {
        public static StormTopology createTopology() {
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            topologyBuilder.setSpout("sentenceSpout", new SentenceSpout(3000, false));
            topologyBuilder.setBolt("consolePrinterBolt", new ConsolePrinterBolt(true), 2).shuffleGrouping("sentenceSpout");

            Config config = new Config();
            config.setDebug(false);

            return topologyBuilder.createTopology();
        }
    }

    public static class ShuffleStreamGroupingWithStreamIdTopology {
        public static StormTopology createTopology() {
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            topologyBuilder.setSpout("sentenceSpout", new SentenceSpoutWithStreamId(3000, false));
            topologyBuilder.setBolt("consolePrinterBolt", new ConsolePrinterBolt(true), 2).shuffleGrouping("sentenceSpout", SentenceSpoutWithStreamId.STREAM_ID);

            Config config = new Config();
            config.setDebug(false);

            return topologyBuilder.createTopology();
        }
    }
}
