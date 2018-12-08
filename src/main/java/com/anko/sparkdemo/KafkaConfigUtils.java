package com.anko.sparkdemo;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
    private static final String DEFAULT_KAFKA_INPUT_LOG_TOPIC = "log-stream";
    private static final String DEFAULT_KAFKA_OUTPUT_LOG_STAT_TOPIC = "log-stat";
    private static final String DEFAULT_KAFKA_OUTPUT_ERROR_ALARM_TOPIC = "error-alarm";

    public static Map<String, Object> createKafkaConsumerConfig(String kafkaUrl) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "use_a_separate_group_id_for_each_stream");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return kafkaParams;
    }

    public static Properties createKafkaProducerConfig(String kafkaUrl) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    public static String getKafkaBrokerUrl() {
        String kafkaUrl = System.getenv("KAFKA_BROKER_URL");
        if(StringUtils.isBlank(kafkaUrl)) {
            throw new IllegalStateException("KAFKA_BROKER_URL environment variable must be set");
        }
        return kafkaUrl;
    }

    public static Collection<String> getKafkaInputTopics() {
        String inputLogTopic = System.getenv("INPUT_LOG_TOPIC");
        if(StringUtils.isBlank(inputLogTopic)) {
            inputLogTopic = DEFAULT_KAFKA_INPUT_LOG_TOPIC;
        }
        return Arrays.asList(inputLogTopic);
    }

    public static String getKafkaLogRateOutputTopic() {
        String outputLogStatTopic = System.getenv("OUTPUT_LOG_STAT_TOPIC");
        if(StringUtils.isBlank(outputLogStatTopic)) {
            outputLogStatTopic = DEFAULT_KAFKA_OUTPUT_LOG_STAT_TOPIC;
        }
        return outputLogStatTopic;
    }

    public static String getKafkaErrorAlarmOutputTopic() {
        String outputErrorAlarmTopic = System.getenv("OUTPUT_ERROR_ALARM_TOPIC");
        if(StringUtils.isBlank(outputErrorAlarmTopic)) {
            outputErrorAlarmTopic = DEFAULT_KAFKA_OUTPUT_ERROR_ALARM_TOPIC;
        }
        return outputErrorAlarmTopic;
    }
}
