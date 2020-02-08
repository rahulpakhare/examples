package com.example.storm.spout;

import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rahul on 3/2/15.
 */
public class DirectEmittingSentenceSpout extends SentenceSpout {
    private static final Logger log = LoggerFactory.getLogger(DirectEmittingSentenceSpout.class);

    private Integer [] taskIds = new Integer[]{};
    private String boltComponentId;
    private int lastTaskIdUsed;

    public DirectEmittingSentenceSpout(long sleep, boolean emmitMessageId, String boltComponentId) {
        super(sleep, emmitMessageId);
        this.boltComponentId = boltComponentId;
    }

    @Override
    public void emit(Values values, Object messageId) {
        if(taskIds.length == 0) {
            taskIds = context.getComponentTasks(boltComponentId).toArray(taskIds);
            log.info("Component {} found task {} ", boltComponentId, taskIds);
        }

        int taskId = -1;
        for(Integer id : taskIds) {
            if(id != lastTaskIdUsed) {
                taskId = id;
            }
        }

        if (messageId == null) {
            collector.emitDirect(taskId, values);
        } else {
            collector.emitDirect(taskId, values, messageId);
        }

        lastTaskIdUsed = taskId;
        log.info("Task id {} is used to emit tuple", taskId);
    }
}
