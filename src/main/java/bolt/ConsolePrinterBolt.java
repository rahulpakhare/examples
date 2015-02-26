package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.LogUtil;

import java.util.Map;

/**
 * Created by rahulpa on 2/26/2015.
 */
public class ConsolePrinterBolt implements IRichBolt {
    private static final Logger log = LoggerFactory.getLogger(ConsolePrinterBolt.class);
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        log.info(LogUtil.logMsgFormat, "ConsolePrinterBolt.prepare method called");
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        log.info("Tuple received : {}", sentence);
    }

    @Override
    public void cleanup() {
        log.info(LogUtil.logMsgFormat, "ConsolePrinterBolt.cleanup method called");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        log.info(LogUtil.logMsgFormat, "ConsolePrinterBolt.declareOutputFields method called");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        log.info(LogUtil.logMsgFormat, "ConsolePrinterBolt.getComponentConfiguration method called");

        return null;
    }
}
