package com.example.storm.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.example.storm.bolt.ConsolePrinterBolt;
import com.example.storm.spout.SentenceSpout;

/**
 * Created by rahul on 3/2/15.
 */
public class FieldGroupingTopology {
    public static void main(String args[]) throws AlreadyAliveException, InvalidTopologyException {

        if(args.length > 1) {
            throw new IllegalArgumentException("Provide Topology name");
        }

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("fieldGroupingSentenceSpout", new SentenceSpout(3000, false));
        topologyBuilder.setBolt("consoleBolt", new ConsolePrinterBolt(true), 2).fieldsGrouping("fieldGroupingSentenceSpout", new Fields("sentence"));

        Config config = new Config();
        config.setDebug(false);

        StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
    }
}
