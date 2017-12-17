package mf.test.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

@Configuration
public class AppConfig {

    @Bean
    public SparkSession sparkSession(JavaSparkContext javaSparkContext){
        return SparkSession.builder().config(javaSparkContext.getConf()).getOrCreate();
    }

    @Bean
    @Profile("local")
    public JavaSparkContext scLocal() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("Indexer");
        return new JavaSparkContext(conf);
    }
    @Bean
    @Profile("prod")
    public JavaSparkContext scProd() {
        SparkConf conf = new SparkConf();
        conf.setAppName("Indexer");
        return new JavaSparkContext(conf);
    }

}
