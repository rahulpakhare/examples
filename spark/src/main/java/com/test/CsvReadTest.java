package com.test;

import com.test.writer.SparkWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class CsvReadTest {
    public static void main(String[] args){
        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        Function toObject = (Function<?, Object>) value -> value;

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
       // System.out.println(textFile.flatMap(s -> Arrays.asList(s.split("\\s+")).iterator()).collect());
        Dataset<String> dataSet = sparkSession.read().textFile("/Volumes/B/mplatform/files/tapad/mPlatform_XAX_NA_ids_full_20190709_000000_000000000072.gz.be66174f-b24f-484f-9e54-ae6d60be85a4");
       // javaRdd(dataSet, toObject);
       // wordCount(sparkSession, conf, dataSet, toObject);
        //
    }

    private static void wordCount(SparkSession sparkSession, SparkConf conf, Dataset<String> dataSet, Function toObject) {
/*        // Create a Java version of the Spark Context
           JavaSparkContext sc = new JavaSparkContext(conf);
        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        JavaRDD textFile = sparkSession.textFile("/Volumes/B/mplatform/files/tapad/1_to_1_OnlyM").javaRDD().map(toObject);
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.foreach(p -> System.out.println(p));
        System.out.println("Total words: " + counts.count());
        counts.saveAsTextFile("/tmp/textWordCount");*/
    }

    private static void javaRdd(Dataset<String> dataSet, Function toObject) {
        JavaRDD textFileRdd = dataSet
                .javaRDD().map(toObject);
        SparkWriter sparkWriter = new SparkWriter();
        System.out.println(textFileRdd.getNumPartitions());
        textFileRdd.mapPartitions(sparkWriter).collect();
        System.out.println(textFileRdd.collect());
    }
}
