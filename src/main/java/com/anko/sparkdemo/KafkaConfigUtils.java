package com.anko.sparkdemo;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Andrey on 03.12.2018.
 */
public class KafkaConfigUtils {
    public static final String KAFKA_LOG_INPUT_TOPIC = "ankotest";
    public static final String KAFKA_LOG_RATE_OUTPUT_TOPIC = "ankotest2";
    public static final String KAFKA_ERROR_ALARM_OUTPUT_TOPIC = "ankotest3";

    public static Map<String, Object> createKafkaConsumerConfig(String kafkaUrl) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaUrl);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        return kafkaParams;
    }

    public static Collection<String> getKafkaInputTopics() {
        return Arrays.asList(KAFKA_LOG_INPUT_TOPIC);
    }

    public static Properties createKafkaProducerConfig(String kafkaUrl) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }
}
