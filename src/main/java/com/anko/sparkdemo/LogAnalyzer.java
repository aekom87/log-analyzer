package com.anko.sparkdemo;

import com.anko.sparkdemo.model.HostLevelKey;
import com.anko.sparkdemo.model.Log;
import com.anko.sparkdemo.model.LogStat;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by Andrey on 03.12.2018.
 */
public class LogAnalyzer {

    private static final Double ERROR_RATE_THRESHOLD = 1.0;

    public static JavaPairDStream<HostLevelKey, LogStat> computeLogStat(
            JavaInputDStream<ConsumerRecord<String, String>> inputLogStream,
            Duration windowLength, Duration windowSlide) {
        return computeLogStat(inputLogStream.map(ConsumerRecord::value), windowLength, windowSlide);
    }

    public static JavaPairDStream<HostLevelKey, LogStat> computeLogStat(
            JavaDStream<String> inputLogStream,
            Duration windowLength, Duration windowSlide) {
        ObjectMapper objectMapper = new ObjectMapper();
        return inputLogStream
                .map(log -> objectMapper.readValue(log, Log.class))
                .mapToPair(log -> new Tuple2<>(HostLevelKey.of(log.getHost(), log.getLevel()), 1))
                .reduceByKeyAndWindow((x, y) -> x + y, windowLength, windowSlide)
                .mapToPair(hostlevelCount -> new Tuple2<>(
                        hostlevelCount._1(),
                        LogStat.of(hostlevelCount._2(), getRate(hostlevelCount._2(), windowLength))));
    }

    private static Double getRate(Integer integer, Duration windowLength) {
        return integer * 1000.0 / windowLength.milliseconds();
    }

    public static JavaPairDStream<HostLevelKey, LogStat> getErrorAlarms(
            JavaPairDStream<HostLevelKey, LogStat> logStatStream) {
        return logStatStream.filter(LogAnalyzer::checkErrorThreshold);
    }

    private static Boolean checkErrorThreshold(Tuple2<HostLevelKey, LogStat> hostLevelLogStat) {
        return hostLevelLogStat._1().getLevel().equals(Log.Level.ERROR)
                && (hostLevelLogStat._2().getRate().compareTo(ERROR_RATE_THRESHOLD) > 0);
    }
}
