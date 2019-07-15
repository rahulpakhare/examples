package com.test;

import com.test.writer.SparkWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class CsvReadTest {
    public static void main(String[] args){

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        Function toObject = (Function<?, Object>) value -> value;

        // Create a Java version of the Spark Context
     //   JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        // Load the text into a Spark RDD, which is a distributed representation of each line of text
      //  JavaRDD textFile = sc.textFile("/Volumes/B/mplatform/files/tapad/1_to_1_OnlyM").javaRDD().map(toObject);
       // System.out.println(textFile.flatMap(s -> Arrays.asList(s.split("\\s+")).iterator()).collect());
        JavaRDD textFileRdd = sparkSession.read().textFile("/Volumes/B/files/tapad/be66174f-b24f-484f-9e54-ae6d60be85a4")
                .javaRDD().map(toObject);
        SparkWriter sparkWriter = new SparkWriter();
        System.out.println(textFileRdd.getNumPartitions());
        textFileRdd.mapPartitions(sparkWriter).collect();
        System.out.println(textFileRdd.collect());
        /*JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.foreach(p -> System.out.println(p));
        System.out.println("Total words: " + counts.count());
        counts.saveAsTextFile("/tmp/textWordCount");*/
    }
}
