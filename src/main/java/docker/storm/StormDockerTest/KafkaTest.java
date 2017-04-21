package docker.storm.StormDockerTest;
import java.util.Arrays;
import java.util.HashMap;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;

import scala.Tuple2;


public class KafkaTest {
	  private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) {
		  SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaTest");
		    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));
		    int numThreads = Integer.parseInt("1");
		    Map<String, Integer> topicMap = new HashMap<>();
		   
		   topicMap.put("test",numThreads);

		   // Direct integration with Kafka without receiver
		    JavaPairReceiverInputDStream<String, String> messages =
		            KafkaUtils.createStream(jssc, "192.168.99.100", "1", topicMap);

		  
		    
		 //   JavaDStream<String> lines = messages.map(tuple2-> tuple2._2());
		    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
		      @Override
		      public String call(Tuple2<String, String> tuple2) {
		        return tuple2._2();
		      }
		    });
		    
		    JavaDStream<String> linesFiltered= lines.filter(s-> s.equals("hello world"));

		    JavaDStream<String> words = linesFiltered.flatMap(new FlatMapFunction<String, String>() {
				@Override
				public Iterable<String> call(String x) throws Exception {
					  return Arrays.asList(SPACE.split(x));
				}

		    });

		    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
		      new PairFunction<String, String, Integer>() {
		        @Override
		        public Tuple2<String, Integer> call(String s) {
		          return new Tuple2<>(s, 1);
		        }
		      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
		        @Override
		        public Integer call(Integer i1, Integer i2) {
		          return i1 + i2;
		        }
		      });

		    wordCounts.print();
		    jssc.start();
		    jssc.awaitTermination();
	}
	
  

}
