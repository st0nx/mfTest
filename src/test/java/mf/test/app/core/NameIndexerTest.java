package mf.test.app.core;

import mf.test.app.AppConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {AppConfig.class, TestConfig.class, FileLoader.class, NameIndexer.class})
public class NameIndexerTest {
    @Autowired
    private JavaSparkContext context;

    @Autowired
    private NameIndexer nameIndexer;

    @Test
    public void checkEqualReads() {
        JavaRDD<Tuple2<String, String>> javaRDD = context.parallelize(Arrays.asList(
                new Tuple2<>("Yana A. Petrova", "1990.01.01"),
                new Tuple2<>("Jana Alex. Petrovva", "1990.1.1")));
        List<Tuple3<Date, String, Integer>> collect = nameIndexer.indexing(javaRDD).collect();
        Assert.assertEquals(collect.get(0)._3(), collect.get(1)._3());
    }

    @Test
    public void checkEqualReadsBrokenOrder() {
        JavaRDD<Tuple2<String, String>> javaRDD = context.parallelize(Arrays.asList(
                new Tuple2<>("Yana A. Petrova", "1990.01.01"),
                new Tuple2<>("Jana Petrovva Alex", "1990.1.1")));
        List<Tuple3<Date, String, Integer>> collect = nameIndexer.indexing(javaRDD).collect();
        Assert.assertEquals(collect.get(0)._3(), collect.get(1)._3());
    }

    @Test
    public void checkEqualReadsWithoutName() {
        JavaRDD<Tuple2<String, String>> javaRDD = context.parallelize(Arrays.asList(
                new Tuple2<>("Ksenija, Ivanoff", "1990.01.01"),
                new Tuple2<>("Ivanova, Xeniya Pavlovna", "1990.1.1")));
        List<Tuple3<Date, String, Integer>> collect = nameIndexer.indexing(javaRDD).collect();
        Assert.assertEquals(collect.get(0)._3(), collect.get(1)._3());
    }

    @Test
    public void checkEqualReadsOtherDelimiter() {
        JavaRDD<Tuple2<String, String>> javaRDD = context.parallelize(Arrays.asList(
                new Tuple2<>("Yana A/, Petrova", "1990.01.01"),
                new Tuple2<>("Petrovva,, Alex", "1990.1.1")));
        List<Tuple3<Date, String, Integer>> collect = nameIndexer.indexing(javaRDD).collect();
        Assert.assertEquals(collect.get(0)._3(), collect.get(1)._3());
    }

    @Test
    public void checkNotEqualOtherDate() {
        JavaRDD<Tuple2<String, String>> javaRDD = context.parallelize(Arrays.asList(
                new Tuple2<>("Yana A/, Petrova", "1990.01.01"),
                new Tuple2<>("Petrovva,, Alex", "1990.1.2")));
        List<Tuple3<Date, String, Integer>> collect = nameIndexer.indexing(javaRDD).collect();
        Assert.assertNotEquals(collect.get(0)._3(), collect.get(1)._3());
    }

    @Test
    public void checkNotEqualBigDistance() {
        JavaRDD<Tuple2<String, String>> javaRDD = context.parallelize(Arrays.asList(
                new Tuple2<>("Yana AAAleSXXx/, Petrova", "1990.01.01"),
                new Tuple2<>("Ivanov Petrovva,, Ksenia", "1990.1.1")));
        List<Tuple3<Date, String, Integer>> collect = nameIndexer.indexing(javaRDD).collect();
        Assert.assertNotEquals(collect.get(0)._3(), collect.get(1)._3());
    }


}
