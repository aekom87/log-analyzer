package com.anko.sparkdemo;

import static com.anko.sparkdemo.KafkaConfigUtils.KAFKA_LOG_RATE_OUTPUT_TOPIC;

import com.anko.sparkdemo.model.HostLevelKey;
import com.anko.sparkdemo.model.Log;
import com.anko.sparkdemo.model.LogStat;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
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

/**
 * Hello world!
 *
 */
public class App 
{
    private static final String SPARK_APP_NAME = "My Kafka streaming app";
    private static final Duration BATCH_DURATION = Durations.seconds(1);
    private static final Duration WINDOW_LENGTH = Durations.seconds(3);
    private static final Duration WINDOW_SLIDE = Durations.seconds(3);

    public static void main( String[] args )
    {
        String kafkaUrl = System.getenv("KAFKA_BROKER_URL");
        if(StringUtils.isBlank(kafkaUrl)) {
            throw new IllegalStateException("KAFKA_BROKER_URL environment variable must be set");
        }

        SparkConf conf = new SparkConf().setAppName(SPARK_APP_NAME);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, BATCH_DURATION);

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(KafkaConfigUtils.getKafkaInputTopics(),
                        KafkaConfigUtils.createKafkaConsumerConfig(kafkaUrl)));

        ObjectMapper objectMapper = new ObjectMapper();

        JavaPairDStream<HostLevelKey, LogStat> tmp = messages
                .map(ConsumerRecord::value)
                .map(log -> objectMapper.readValue(log, Log.class))
                .mapToPair(log -> new Tuple2<>(HostLevelKey.of(log.getHost(), log.getLevel()), 1))
                .reduceByKeyAndWindow((x, y) -> x + y, WINDOW_LENGTH, WINDOW_SLIDE)
                .mapToPair(hostlevelCount -> new Tuple2<>(hostlevelCount._1(),
                        LogStat.of(hostlevelCount._2(), hostlevelCount._2() * 1000.0 / WINDOW_LENGTH.milliseconds())));

        tmp.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                Producer<String, String> producer =
                        new KafkaProducer<>(KafkaConfigUtils.createKafkaProducerConfig(kafkaUrl));
                while (partition.hasNext()) {
                    Tuple2<HostLevelKey, LogStat> hostLevelStat = partition.next();
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            "ankotest2",
                            hostLevelStat._1().getHost()+"/"+hostLevelStat._1().getLevel()+
                            ":"+hostLevelStat._2().getCount());
                    producer.send(record);
                }
            });
        });

//        tmp.filter(hostLevelLogStat -> (hostLevelLogStat._1().getLevel().equals(Log.Level.ERROR) &&
//                hostLevelLogStat._2().getRate() > 1.0)).print();

        ssc.start();
        try {
            ssc.awaitTerminationOrTimeout(30000);
        } catch (InterruptedException e) {
            System.out.println("KABOOM");
        }
    }
}
