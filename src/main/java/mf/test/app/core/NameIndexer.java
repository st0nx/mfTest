package mf.test.app.core;

import mf.test.app.string.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class NameIndexer {


    @Value("${indexer.distance:12}")
    private Double distance;

    @Autowired
    private JavaSparkContext context;

    public JavaRDD<Tuple3<Date, String, Integer>> indexing(JavaRDD<Tuple2<String, String>> lines) {
        Broadcast<Double> broadcast = context.broadcast(distance);
        return lines.mapToPair((r) -> {
            SimpleDateFormat sdfmt1 = new SimpleDateFormat("yyyy.MM.dd");
            Date parse = sdfmt1.parse(r._2);
            String string = StringUtils.normalizeString(r._1);
            return new Tuple2<>(parse.getTime() + string, new Tuple2<>(parse, string));
        }).sortByKey()
                .mapToPair((v) -> v._2)
                .groupByKey()
                .flatMap((t) -> {
                    List<Tuple3<Date, String, Integer>> val = new ArrayList<>();
                    Iterator<String> iterator = t._2.iterator();
                    String last = null;
                    int index = makeIndex();
                    while (iterator.hasNext()) {
                        String str = iterator.next();
                        if(last == null) {
                            val.add(new Tuple3<>(t._1, str, index));
                        } else {
                            if(StringUtils.countDinst(last, str) <= broadcast.getValue()) {
                                val.add(new Tuple3<>(t._1, str, index));
                            } else {
                                index = makeIndex();
                                val.add(new Tuple3<>(t._1, str, index));
                            }
                        }
                        last = str;
                    }
                    return val.iterator();
                });
    }


    private static int makeIndex() {
        return ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE);
    }
}
