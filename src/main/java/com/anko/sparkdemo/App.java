package com.anko.sparkdemo;

import com.anko.sparkdemo.model.HostLevelKey;
import com.anko.sparkdemo.model.HostLevelLogStat;
import com.anko.sparkdemo.model.LogStat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final String SPARK_APP_NAME = "My Kafka streaming app";
    private static final Duration WINDOW_LENGTH = Durations.seconds(60);
    private static final Duration WINDOW_SLIDE = Durations.seconds(10);

    public static void main( String[] args ) throws InterruptedException {
        String kafkaUrl = KafkaConfigUtils.getKafkaBrokerUrl();

        SparkConf conf = new SparkConf().setAppName(SPARK_APP_NAME);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, WINDOW_SLIDE);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(
                        KafkaConfigUtils.getKafkaInputTopics(),
                        KafkaConfigUtils.createKafkaConsumerConfig(kafkaUrl)));

        JavaPairDStream<HostLevelKey, LogStat> logStatStream = LogAnalyzer.computeLogStat(
                messages, WINDOW_LENGTH, WINDOW_SLIDE);
        sendLogStatToKafka(logStatStream, kafkaUrl, KafkaConfigUtils.getKafkaLogRateOutputTopic());

        JavaPairDStream<HostLevelKey, LogStat> alarmingErrorStatStream = LogAnalyzer.getErrorAlarms(
                logStatStream);
        sendLogStatToKafka(alarmingErrorStatStream, kafkaUrl, KafkaConfigUtils.getKafkaErrorAlarmOutputTopic());

        ssc.start();
        ssc.awaitTerminationOrTimeout(30000);
    }

    private static void sendLogStatToKafka(JavaPairDStream<HostLevelKey, LogStat> logStatStream,
                                           String kafkaUrl,
                                           String kafkaTopic) {
        logStatStream.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                Producer<String, String> producer =
                        new KafkaProducer<>(KafkaConfigUtils.createKafkaProducerConfig(kafkaUrl));
                sendPartitionToKafka(partition, kafkaTopic, producer);
            });
        });
    }

    private static void sendPartitionToKafka(Iterator<Tuple2<HostLevelKey, LogStat>> partition,
                                             String kafkaTopic,
                                             Producer<String, String> producer)
            throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        while (partition.hasNext()) {
            Tuple2<HostLevelKey, LogStat> hostLevelStat = partition.next();
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    kafkaTopic,
                    objectMapper.writeValueAsString(HostLevelLogStat.of(
                            hostLevelStat._1(),
                            hostLevelStat._2()))
            );
            producer.send(record);
        }
    }
}
