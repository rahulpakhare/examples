package com.example.storm.topology;

import com.example.storm.bolt.ConsolePrinterBolt;
import com.example.storm.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by rahul on 3/2/15.
 */
public class NoneGroupingTopology {
    public static void main(String args[]) throws Exception {
        if(args.length > 1) {
            throw new IllegalArgumentException("Provide Topology name");
        }

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("sentenceSpout", new SentenceSpout(3000, false));
        topologyBuilder.setBolt("consoleBolt", new ConsolePrinterBolt(true), 2).noneGrouping("sentenceSpout");

        Config config = new Config();
        config.setDebug(false);

        StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
    }
}
