package com.example.storm.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.example.storm.bolt.ConsolePrinterBolt;
import com.example.storm.spout.SentenceSpout;

/**
 * Created by rahulpa on 2/26/2015.
 */
public class SpoutAckFailMethodCallTesTopology {

    public static void main(String args[]) throws AlreadyAliveException, InvalidTopologyException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Please specify true/false to ack tuples in bolt. If true spout ack method callback will call. If false spout fail method callback will call");
        }
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("SentenceSpout", new SentenceSpout(3000, true), 1);
        builder.setBolt("ConsolePrinterBolt", new ConsolePrinterBolt(Boolean.valueOf(args[0])), 1)
                .localOrShuffleGrouping("SentenceSpout");

        Config config = new Config();
        config.setMaxSpoutPending(12);
        config.setNumWorkers(5);
        config.setNumAckers(1);
        config.setDebug(false);

        StormSubmitter.submitTopology(args[1], config, builder.createTopology());
    }
}
