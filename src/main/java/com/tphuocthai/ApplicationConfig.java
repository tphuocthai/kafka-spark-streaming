package com.tphuocthai;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by tphuocthai on 7/2/17.
 */
public class ApplicationConfig {

    Config conf = ConfigFactory.load();

    public String getKafkaTopics() {
        return conf.getString("kafka.topics");
    }


    public String getAppName() {
        return conf.getString("spark.appName");
    }

    public String getSparkMaster() {
        return conf.getString("spark.master");
    }

    public Object getKafkaBootstrapServer() {
        return conf.getString("kafka.bootstrap.server");
    }

    public Object getKafkaGroupId() {
        return conf.getString("kafka.group_id");
    }
}
