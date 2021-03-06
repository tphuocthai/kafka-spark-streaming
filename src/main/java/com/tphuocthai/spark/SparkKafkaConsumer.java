package com.tphuocthai.spark;

import com.tphuocthai.config.ApplicationConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tphuocthai on 7/2/17.
 */
public class SparkKafkaConsumer {

    public static void startStream() throws InterruptedException {

        ApplicationConfig config = new ApplicationConfig();

        SparkConf conf = new SparkConf().setAppName(config.getAppName()).setMaster(config.getSparkMaster());
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(2));

        Map<String, Object> kafkaParams = new HashMap() {{
            put("bootstrap.servers", config.getKafkaBootstrapServer());
            put("key.deserializer", StringDeserializer.class);
            put("value.deserializer", StringDeserializer.class);
            put("group.id", config.getKafkaGroupId());
            put("auto.offset.reset", "latest");
            put("enable.auto.commit", false);
        }};
        Collection<String> topics = Arrays.asList(config.getKafkaTopics().split(","));

        JavaInputDStream<ConsumerRecord<Object, Object>> dStream = KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        dStream.flatMap(r -> Arrays.asList(r.value().toString().split(" ")).iterator())
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2)
                .print();

        ssc.start();
        ssc.awaitTermination();
    }
}
