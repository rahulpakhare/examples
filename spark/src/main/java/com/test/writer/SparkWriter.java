package com.test.writer;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SparkWriter implements FlatMapFunction<Iterator<Object>, Map<String, Serializable>>   {

    @Override
    public Iterator<Map<String, Serializable>> call(Iterator<Object> objectIterator) {
        System.out.println("in call method");
        while (objectIterator.hasNext()) {
            try {
                Object item = objectIterator.next();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        List<Map<String, Serializable>> list = new ArrayList();
        return list.iterator();
    }
}
