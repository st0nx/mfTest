package mf.test.app.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TestConfig {

    @Bean
    public JavaSparkContext sc() {
        SparkConf conf = new SparkConf().setAppName("Indexer test").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

}
