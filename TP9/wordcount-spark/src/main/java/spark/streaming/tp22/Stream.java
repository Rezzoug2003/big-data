package spark.streaming.tp22;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import java.util.Arrays;

public class Stream {
    public static void main(String[] args) throws InterruptedException {
        // 1. Config
        SparkConf conf = new SparkConf()
                .setAppName("NetworkWordCount")
                .setMaster("local[*]"); // Use local to run inside the container

        // 2. Create Context with a 5-second batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 3. Create a DStream that connects to localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        // 4. Perform Word Count
        JavaPairDStream<String, Integer> wordCounts = lines
                .flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        // 5. Print result to console
        wordCounts.print();

        // 6. Start the streaming
        jssc.start();
        jssc.awaitTermination();
    }
}