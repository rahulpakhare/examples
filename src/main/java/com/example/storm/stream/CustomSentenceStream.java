package com.example.storm.stream;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.example.storm.util.LogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rahul on 3/3/15.
 */
public class CustomSentenceStream implements CustomStreamGrouping, Serializable {
    private static final Logger log = LoggerFactory.getLogger(CustomSentenceStream.class);

    private Integer firstTask;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        log.info(LogUtil.logMsgFormat, "Task ids " + list);
        log.info(LogUtil.logMsgFormat, "Global stream id " + globalStreamId);
        firstTask = list.get(0);
        log.info(LogUtil.logMsgFormat, String.format("Task %d selected for sending tuple", firstTask));
    }

    @Override
    public List<Integer> chooseTasks(int id, List<Object> list) {
        log.info(LogUtil.logMsgFormat, "Id from choose task " + id);
        log.info(LogUtil.logMsgFormat, "List from choose task " + list);

        List<Integer> taskIds = new ArrayList<>();
        taskIds.add(firstTask);

        return taskIds;
    }
}
