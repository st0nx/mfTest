package mf.test.app;

import mf.test.app.core.FileLoader;
import mf.test.app.core.NameIndexer;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;
import scala.Tuple2;

@Service
public class ConsoleRunner implements CommandLineRunner {

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private NameIndexer nameIndexer;

    private static final Logger log = LoggerFactory.getLogger(ConsoleRunner.class);

    @Override
    public void run(String... args) throws Exception {
        if(args.length!=2)
            throw new IllegalArgumentException("Input format: <file_path_in.csv> <dir_path_out>");

        JavaRDD<Tuple2<String, String>> rdd = fileLoader.loadCsv(args[0])
                .map((r) -> new Tuple2<>(r.getString(0), r.getString(1)));
        fileLoader.writeFile(args[1], nameIndexer.indexing(rdd));
    }
}
