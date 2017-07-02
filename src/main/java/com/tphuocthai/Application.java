package com.tphuocthai;

import com.tphuocthai.spark.SparkKafkaConsumer;

/**
 * Created by tphuocthai on 7/2/17.
 */
public class Application {

    public static void main(String[] args) throws InterruptedException {
        SparkKafkaConsumer.startStream();
    }
}
