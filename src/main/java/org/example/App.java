package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Arrays;

public class App {
    public static void main(String[] args) throws InterruptedException {

        System.out.println("Hello world!");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("task").setMaster("local[6]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(9));
        JavaReceiverInputDStream<String> data = jsc.socketTextStream("localhost",9999);
git 
        JavaDStream<String> d = data.flatMap(data1-> Arrays.asList(data1.split(" ")).iterator());
        JavaPairDStream<String,Long> d2 = d.mapToPair(data2 -> new Tuple2<String ,Long>(data2,1L));
        JavaPairDStream<String,Long> newData = d2.reduceByKey((x,y)->x+y);

        newData.print();
        jsc.start();
        jsc.awaitTermination();
    }
}