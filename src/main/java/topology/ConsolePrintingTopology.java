package topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import bolt.ConsolePrinterBolt;
import spout.SentenceSpout;

/**
 * Created by rahulpa on 2/26/2015.
 */
public class ConsolePrintingTopology {

    public static void main(String args[]) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("SentenceSpout", new SentenceSpout(3000, false), 1);
        builder.setBolt("ConsolePrinterBolt", new ConsolePrinterBolt(), 1)
                .localOrShuffleGrouping("SentenceSpout");

        Config config = new Config();
        config.setMaxSpoutPending(12);
        config.setNumWorkers(5);
        config.setNumAckers(1);
        config.setDebug(true);

        StormSubmitter.submitTopology(args[0], config, builder.createTopology());
    }
}
