package com.example.storm.bolt;

import com.example.storm.util.LogUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created on 2/26/2015.
 */
public class ConsolePrinterBolt implements IRichBolt {
    private static final Logger log = LoggerFactory.getLogger(ConsolePrinterBolt.class);
    private OutputCollector outputCollector;
    private boolean ack;
    private int taskIndex;

    public ConsolePrinterBolt(boolean ack) {
        this.ack = ack;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        log.info(LogUtil.logMsgFormat, "ConsolePrinterBolt_"+taskIndex +".prepare method called");
        this.outputCollector = outputCollector;
        taskIndex = topologyContext.getThisTaskId();
        log.info("This component id {} ", topologyContext.getThisComponentId());
    }

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        log.info("ConsolePrinterBolt_" + taskIndex + " Tuple received : {} from stream {} ", sentence, tuple.getSourceStreamId());
        if (ack) {
            outputCollector.ack(tuple);
        }

    }

    @Override
    public void cleanup() {
        log.info(LogUtil.logMsgFormat, "ConsolePrinterBolt_"+taskIndex +".cleanup method called");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        log.info(LogUtil.logMsgFormat, "ConsolePrinterBolt_"+taskIndex +".declareOutputFields method called");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        log.info(LogUtil.logMsgFormat, "ConsolePrinterBolt_"+taskIndex +".getComponentConfiguration method called");

        return null;
    }
}