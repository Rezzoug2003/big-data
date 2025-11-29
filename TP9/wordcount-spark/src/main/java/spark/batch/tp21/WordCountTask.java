package spark.batch.tp21;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import java.util.Arrays;
import com.google.common.base.Preconditions;

public class WordCountTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);

    public static void main(String[] args) {
        // Check if input and output arguments are provided
        Preconditions.checkArgument(args.length > 1, "Please provide the path of input file and output dir as parameters.");
        new WordCountTask().run(args[0], args[1]);
    }

    public void run(String inputFilePath, String outputDir) {
        // Setup Spark Configuration (Note: We removed .setMaster("local") so it works on clusters)
        SparkConf conf = new SparkConf()
                .setAppName(WordCountTask.class.getName());

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. Read file
        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        // 2. Map and Reduce logic
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator()) // Split by space
                .mapToPair(word -> new Tuple2<>(word, 1))             // Assign 1 to every word
                .reduceByKey((a, b) -> a + b);                        // Sum the counts

        // 3. Save result
        counts.saveAsTextFile(outputDir);
        
        // Stop the context
        sc.close();
    }
}