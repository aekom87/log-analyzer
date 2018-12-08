# Log analyzer

Spark Streaming application for real time log analysis.

## Getting Started

1. Clone project
2. Build using Maven:
```
mvn clean install
```
3. Set KAFKA_BROKER_URL environment variable
```
export KAFKA_BROKER_URL=[IP]:[PORT]
```
4. Set Kafka topics (or use default ones)

Environment variable | Description | Default value
-------------------- |-------------|----------------
INPUT_LOG_TOPIC | Kafka topic to read logs from | log-stream
OUTPUT_LOG_STAT_TOPIC | Kafka topic to send log stats to |  log-stat
OUTPUT_ERROR_ALARM_TOPIC | Kafka topic to send error alarms to | error-alarm


5. Run using Spark:
 ```
 ./bin/spark-submit --class com.anko.sparkdemo.App --master spark://[MASTER_IP]:[MASTER_PORT] [PATH_TO_JAR]/kafkastreaming-1.0-SNAPSHOT-jar-with-dependencies.jar
 ```

### Prerequisites
1. Spark version 2.0.2
2. Spark standalone cluster running with at least 1 worker instance
3. Kafka version 0.10.0.1 running

## Running the tests

Send logs to [INPUT_LOG_TOPIC] Kafka topic. Expected format:
```
{  
   "records":[  
      {  
         "value":{  
            "timestamp":1543745608644,
            "host":"www.yandex.ru",
            "level":"ERROR",
            "text":"Test log"
         }
      }
   ]
}
```
Check output in [OUTPUT_LOG_STAT_TOPIC] and [OUTPUT_ERROR_ALARM_TOPIC] topics.