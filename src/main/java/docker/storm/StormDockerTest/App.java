package docker.storm.StormDockerTest;
import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
     // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));
   //     JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("192.168.99.100", 9999);

     // Split each line into words
        JavaDStream<String> words = lines.flatMap(
          new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) {
              return Arrays.asList(x.split(" "));
            }
          });
        
     // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
          new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
              return new Tuple2<String, Integer>(s, 1);
            }
          });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
          new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
              return i1 + i2;
            }
          });

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}
