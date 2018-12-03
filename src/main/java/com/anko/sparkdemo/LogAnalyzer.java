package com.anko.sparkdemo;

import com.anko.sparkdemo.model.HostLevelKey;
import com.anko.sparkdemo.model.Log;
import com.anko.sparkdemo.model.LogStat;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by Andrey on 03.12.2018.
 */
public class LogAnalyzer {

    public static JavaPairDStream<HostLevelKey, LogStat> computeLogStat(
            JavaInputDStream<ConsumerRecord<String, String>> inputLogStream,
            Duration windowLength, Duration windowSlide) {
        ObjectMapper objectMapper = new ObjectMapper();
        return inputLogStream
                .map(ConsumerRecord::value)
                .map(log -> objectMapper.readValue(log, Log.class))
                .mapToPair(log -> new Tuple2<>(HostLevelKey.of(log.getHost(), log.getLevel()), 1))
                .reduceByKeyAndWindow((x, y) -> x + y, windowLength, windowSlide)
                .mapToPair(hostlevelCount -> new Tuple2<>(hostlevelCount._1(),
                        LogStat.of(hostlevelCount._2(), hostlevelCount._2() * 1000.0 / windowLength.milliseconds())));
    }

    public static JavaPairDStream<HostLevelKey, LogStat> checkErrorThreshold(
            JavaPairDStream<HostLevelKey, LogStat> logStatStream) {
        return logStatStream.filter(hostLevelLogStat -> (hostLevelLogStat._1().getLevel().equals(Log.Level.ERROR)
                && hostLevelLogStat._2().getRate() > 1.0));
    }
}
