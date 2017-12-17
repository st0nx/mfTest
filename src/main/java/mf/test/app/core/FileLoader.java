package mf.test.app.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple3;

import java.util.Collections;
import java.util.Date;

@Service
public class FileLoader {

    private static final String DELIMITER = "|";
    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private JavaSparkContext context;

    public JavaRDD<Row> loadCsv(String path) {
        return sparkSession.read().option("header", "true").option("delimiter", DELIMITER).csv(path).toJavaRDD();
    }

    public void writeFile(String path, JavaRDD<Tuple3<Date, String, Integer>> data) {
        JavaRDD<String> map = data.map((t) -> t._3().toString() + DELIMITER + t._2() + DELIMITER + t._1());
        map.saveAsTextFile(path);
    }
}
