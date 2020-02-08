package com.example.storm.spout;


import com.example.storm.util.LogUtil;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rahul on 3/2/15.
 */
public class SentenceSpoutWithStreamId extends SentenceSpout {
    private static final Logger log = LoggerFactory.getLogger(SentenceSpoutWithStreamId.class);
    public static final String STREAM_ID = "lineStream";

    public SentenceSpoutWithStreamId(long sleep, boolean emmitMessageId) {
        super(sleep, emmitMessageId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        log.info(LogUtil.logMsgFormat, "SentenceSpoutWithStreamId.declareOutputFields method called");
        log.info("Creating stream {}", STREAM_ID);
        outputFieldsDeclarer.declareStream(STREAM_ID, new Fields("sentence"));
    }

    @Override
    public void emit(Values values, Object messageId) {
        if(messageId == null) {
            collector.emit(STREAM_ID, values);
        } else {
            collector.emit(STREAM_ID, values, messageId);
        }
    }
}
