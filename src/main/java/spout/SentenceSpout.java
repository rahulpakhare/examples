package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.LogUtil;

import java.util.Map;
import java.util.Random;

/**
 * Created by rahulpa on 2/26/2015.
 */
public class SentenceSpout implements IRichSpout {
private static final Logger log = LoggerFactory.getLogger(SentenceSpout.class);

    private SpoutOutputCollector collector;
    private String[] sentences;
    private long sleep;
    private boolean ack;
    private Random random;
    private long counter;

    public SentenceSpout (long sleep, boolean ack) {
        this.sleep = sleep;
        this.ack = ack;
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

        sentences = new String[] {
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

        if (ack)
            collector.emit(new Values(sentence), counter++);
        else
            collector.emit(new Values(sentence));

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
