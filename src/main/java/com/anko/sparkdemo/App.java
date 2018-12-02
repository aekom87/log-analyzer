package com.anko.sparkdemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf()
                .setAppName("My Kafka streaming app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));

        Map<String, Object> kafkaConf = createKafkaConfig();
        Collection<String> topics = getKafkaTopics();

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaConf));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> streamLogs = messages.map(ConsumerRecord::value);

        ObjectMapper objectMapper = new ObjectMapper();

        JavaPairDStream<HostLevelKey, LogStat> tmp = streamLogs.map(log -> objectMapper.readValue(log, Log.class))
                .mapToPair(log -> new Tuple2<>(HostLevelKey.of(log.getHost(), log.getLevel()), 1))
                .reduceByKeyAndWindow((x, y) -> x + y, Durations.seconds(3), Durations.seconds(3))
                .mapToPair(hostlevelCount -> new Tuple2<>(hostlevelCount._1(),
                        LogStat.of(hostlevelCount._2(), (double)(hostlevelCount._2()) / 3)));
        tmp.print();

        tmp.filter(hostLevelLogStat -> (hostLevelLogStat._1().getLevel().equals(Log.Level.ERROR) &&
                hostLevelLogStat._2().getRate() > 1.0)).print();

        ssc.start();              // Start the computation
        try {
            ssc.awaitTerminationOrTimeout(30000);   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            System.out.println("KABOOM");
        }
    }

    private static Map<String, Object> createKafkaConfig() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.0.2.15:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream666");
        kafkaParams.put("auto.offset.reset", "latest");
        return kafkaParams;
    }

    private static Collection<String> getKafkaTopics() {
        return Arrays.asList("ankotest");
    }

    @Data
    private static class Log implements Serializable {
        private Long timestamp;
        private String host;
        private Level level;
        private String text;

        enum Level {
            TRACE,
            DEBUG,
            INFO,
            WARN,
            ERROR
        }
    }

    @Data(staticConstructor="of")
    private static class LogStat implements Serializable {
        final private Integer count;
        final private Double rate;
    }

    @Data(staticConstructor="of")
    private static class HostLevelKey implements Serializable {
        final private String host;
        final private Log.Level level;
    }
}
