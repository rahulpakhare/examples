package com.example.storm.spout;


import com.example.storm.util.LogUtil;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Created by rahulpa on 2/26/2015.
 */
public class SentenceSpout implements IRichSpout {
private static final Logger log = LoggerFactory.getLogger(SentenceSpout.class);

    protected SpoutOutputCollector collector;
    private String[] sentences;
    private long sleep;
    private boolean emmitMessageId;
    private Random random;
    private long counter;
    protected TopologyContext context;

    public SentenceSpout (long sleep, boolean emmitMessageId) {
        this.sleep = sleep;
        this.emmitMessageId = emmitMessageId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        log.info(LogUtil.logMsgFormat, "SentenceSpout.declareOutputFields method called");
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        log.info(LogUtil.logMsgFormat, "SentenceSpout.open method called");

        this.collector = spoutOutputCollector;
        random = new Random();
        context = topologyContext;
        sentences = getInputFeeds();
    }

    public String[] getInputFeeds() {
        return new String[] {
                "ack and Jill went up the hill",
                "To fetch a pail of water.",
                "Jack fell down and broke his crown",
                "And Jill came tumbling after.",
                "Up Jack got, and home did trot",
                "As fast as he could caper",
                "He went to bed to mend his head",
                "With vinegar and brown paper."
        };
    }

    @Override
    public void nextTuple() {
        if(sleep > 0) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }

        String sentence = sentences[random.nextInt(sentences.length)];

        if (emmitMessageId)
            emit(new Values(sentence), counter++);
        else
            emit(new Values(sentence), null);

    }

    public void emit(Values values, Object messageId) {
        if(messageId == null) {
            collector.emit(values);
        } else {
            collector.emit(values, messageId);
        }
    }

    @Override
    public void close() {
        log.info(LogUtil.logMsgFormat, "SentenceSpout.close method called");
    }

    @Override
    public void activate() {
        log.info(LogUtil.logMsgFormat, "SentenceSpout.activate method called");
    }

    @Override
    public void deactivate() {
        log.info(LogUtil.logMsgFormat, "SentenceSpout.deactivate method called");
    }

    @Override
    public void ack(Object o) {
        log.info(LogUtil.logMsgFormat, "SentenceSpout.ack method called with Message Id " + o);
    }

    @Override
    public void fail(Object o) {
        log.info(LogUtil.logMsgFormat, "SentenceSpout.fail method called with Message Id " + o);
    }
}
