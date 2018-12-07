package com.anko.sparkdemo;

import static com.anko.sparkdemo.model.Log.Level.DEBUG;
import static com.anko.sparkdemo.model.Log.Level.ERROR;
import static com.anko.sparkdemo.model.Log.Level.INFO;
import static com.anko.sparkdemo.model.Log.Level.WARN;
import static org.junit.Assert.assertTrue;

import com.anko.sparkdemo.model.HostLevelKey;
import com.anko.sparkdemo.model.LogStat;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Unit test for simple App.
 */
public class AppTest implements Serializable
{
    @Test
    public void testComputeLogStat()
    {
        JavaStreamingContext ssc = prepareStreamContext("testComputeLogStat");
        JavaDStream<String> streamLogs = generateTestStream(ssc);

        JavaPairDStream<HostLevelKey, LogStat> logStatStream = LogAnalyzer.computeLogStat(streamLogs,
                Durations.seconds(3), Durations.seconds(3));

        List<Tuple2<HostLevelKey, LogStat>> actualResult = collectResult(logStatStream);

        ssc.start();
        try {
            ssc.awaitTerminationOrTimeout(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(actualResult);
        List<Tuple2<HostLevelKey, LogStat>> tmp = prepareExpectedResult();
        System.out.println(tmp);
        assertTrue(actualResult.containsAll(tmp));

        ssc.stop();
    }

    @Test
    public void testCheckErrorThreshold()
    {
        JavaStreamingContext ssc = prepareStreamContext("testCheckErrorThreshold");
        JavaDStream<String> streamLogs = generateTestStream(ssc);
        JavaPairDStream<HostLevelKey, LogStat> logStatStream = LogAnalyzer.computeLogStat(streamLogs,
                Durations.seconds(3), Durations.seconds(3));

        JavaPairDStream<HostLevelKey, LogStat> alarmingErrorStatStream = LogAnalyzer.checkErrorThreshold(
                logStatStream);

        List<Tuple2<HostLevelKey, LogStat>> actualResult = collectResult(alarmingErrorStatStream);

        ssc.start();
        try {
            ssc.awaitTerminationOrTimeout(6000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(actualResult);
        List<Tuple2<HostLevelKey, LogStat>> tmp = prepareExpectedResultErrorThreshold();
        System.out.println(tmp);
        assertTrue(actualResult.containsAll(tmp));

        ssc.stop();
    }

    private static List<Tuple2<HostLevelKey, LogStat>> prepareExpectedResult() {
        List<Tuple2<HostLevelKey, LogStat>> expectedResult = new ArrayList<>();

        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.google.com", INFO), LogStat.of(2, 2.0 / 3)));
        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.yandex.ru", INFO), LogStat.of(1, 1.0 / 3)));
        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.google.com", WARN), LogStat.of(1, 1.0 / 3)));
        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.yandex.ru", ERROR), LogStat.of(4, 4.0 / 3)));
        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.yandex.ru", DEBUG), LogStat.of(1, 1.0 / 3)));
        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.google.com", DEBUG), LogStat.of(1, 1.0 / 3)));

        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.yandex.ru", INFO), LogStat.of(2, 2.0 / 3)));
        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.google.com", WARN), LogStat.of(1, 1.0 / 3)));
        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.google.com", ERROR), LogStat.of(1, 1.0 / 3)));
        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.google.com", INFO), LogStat.of(1, 1.0 / 3)));
        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.google.com", DEBUG), LogStat.of(3, 3.0 / 3)));
        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.yandex.ru", DEBUG), LogStat.of(1, 1.0 / 3)));

        return expectedResult;
    }

    private static List<Tuple2<HostLevelKey, LogStat>> prepareExpectedResultErrorThreshold() {
        List<Tuple2<HostLevelKey, LogStat>> expectedResult = new ArrayList<>();

        expectedResult.add(new Tuple2<>(HostLevelKey.of("www.yandex.ru", ERROR), LogStat.of(4, 4.0 / 3)));

        return expectedResult;
    }

    private static JavaDStream<String> generateTestStream(JavaStreamingContext ssc) {
        Queue<JavaRDD<String>> queue = new LinkedList<>();
        JavaSparkContext sc = ssc.sparkContext();

        queue.add(sc.parallelize(Lists.newArrayList(
                "{\"timestamp\":1543745608644,\"host\":\"www.google.com\",\"level\":\"INFO\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.yandex.ru\",\"level\":\"INFO\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.google.com\",\"level\":\"WARN\",\"text\":\"Test log\"}")));
        queue.add(sc.parallelize(Lists.newArrayList(
                "{\"timestamp\":1543745608644,\"host\":\"www.yandex.ru\",\"level\":\"ERROR\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.google.com\",\"level\":\"INFO\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.yandex.ru\",\"level\":\"DEBUG\",\"text\":\"Test log\"}")));
        queue.add(sc.parallelize(Lists.newArrayList(
                "{\"timestamp\":1543745608644,\"host\":\"www.google.com\",\"level\":\"DEBUG\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.yandex.ru\",\"level\":\"ERROR\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.yandex.ru\",\"level\":\"ERROR\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.yandex.ru\",\"level\":\"ERROR\",\"text\":\"Test log\"}")));
        queue.add(sc.parallelize(Lists.newArrayList(
                "{\"timestamp\":1543745608644,\"host\":\"www.yandex.ru\",\"level\":\"INFO\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.yandex.ru\",\"level\":\"INFO\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.google.com\",\"level\":\"WARN\",\"text\":\"Test log\"}")));
        queue.add(sc.parallelize(Lists.newArrayList(
                "{\"timestamp\":1543745608644,\"host\":\"www.google.com\",\"level\":\"ERROR\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.google.com\",\"level\":\"INFO\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.google.com\",\"level\":\"DEBUG\",\"text\":\"Test log\"}")));
        queue.add(sc.parallelize(Lists.newArrayList(
                "{\"timestamp\":1543745608644,\"host\":\"www.yandex.ru\",\"level\":\"DEBUG\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.google.com\",\"level\":\"DEBUG\",\"text\":\"Test log\"}",
                "{\"timestamp\":1543745608644,\"host\":\"www.google.com\",\"level\":\"DEBUG\",\"text\":\"Test log\"}")));

        return ssc.queueStream(queue, true);
    }

    private static JavaStreamingContext prepareStreamContext(String testAppName) {
        SparkConf conf = new SparkConf()
                .setAppName(testAppName)
                .setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts", "true");;
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
        return ssc;
    }

    private static List<Tuple2<HostLevelKey, LogStat>> collectResult(
            JavaPairDStream<HostLevelKey, LogStat> outputStream) {
        List<Tuple2<HostLevelKey, LogStat>> result = new ArrayList<>();
        outputStream.foreachRDD(rdd -> {
            List<Tuple2<HostLevelKey, LogStat>> tmp = rdd.collect();
            result.addAll(tmp);
        });
        return result;
    }
}
