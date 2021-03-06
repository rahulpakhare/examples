package com.example.storm.topology;

import com.example.storm.bolt.ConsolePrinterBolt;
import com.example.storm.spout.SentenceSpout;
import com.example.storm.stream.CustomSentenceStream;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by rahul on 3/3/15.
 */
public class CustomStreamGroupingTopology {

    public static void main(String args[]) throws Exception {
        if(args.length > 1) {
            throw new IllegalArgumentException("Provide Topology name");
        }

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentenceSpout", new SentenceSpout(3000, false));
        builder.setBolt("consoleBolt", new ConsolePrinterBolt(true), 2).customGrouping("sentenceSpout", new CustomSentenceStream());

        StormSubmitter.submitTopology(args[0], new Config(), builder.createTopology());
    }
}
