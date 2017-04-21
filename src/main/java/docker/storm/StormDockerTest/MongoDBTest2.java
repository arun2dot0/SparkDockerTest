package docker.storm.StormDockerTest;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.BSONFileOutputFormat;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.MongoUpdateWritable;


import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class MongoDBTest2 {
	
	public static void main(String[] args) {
		  JavaSparkContext sc = new JavaSparkContext("local", "Java Word Count");
	        
	      
		// Set configuration options for the MongoDB Hadoop Connector.
		Configuration mongodbConfig = new Configuration();
		// MongoInputFormat allows us to read from a live MongoDB instance.
		// We could also use BSONFileInputFormat to read BSON snapshots.
		mongodbConfig.set("mongo.job.input.format",
		                  "com.mongodb.hadoop.MongoInputFormat");
		// MongoDB connection string naming a collection to use.
		// If using BSON, use "mapred.input.dir" to configure the directory
		// where BSON files are located instead.
		mongodbConfig.set("mongo.input.uri",
		                  "mongodb://localhost:27017/db.collection");

		// Create an RDD backed by the MongoDB collection.
		JavaPairRDD<Object, BSONObject> documents = sc.newAPIHadoopRDD(
		    mongodbConfig,            // Configuration
		    MongoInputFormat.class,   // InputFormat: read from a live cluster.
		    Object.class,             // Key class
		    BSONObject.class          // Value class
		);

		// Create a separate Configuration for saving data back to MongoDB.
		Configuration outputConfig = new Configuration();
		outputConfig.set("mongo.output.uri",
		                 "mongodb://localhost:27017/output.collection");

		// Save this RDD as a Hadoop "file".
		// The path argument is unused; all documents will go to 'mongo.output.uri'.
		documents.saveAsNewAPIHadoopFile(
		    "file:///this-is-completely-unused",
		    Object.class,
		    BSONObject.class,
		    MongoOutputFormat.class,
		    outputConfig
		);

		// We can also save this back to a BSON file.
		documents.saveAsNewAPIHadoopFile(
		    "hdfs://localhost:8020/user/spark/bson-demo",
		    Object.class,
		    BSONObject.class,
		    BSONFileOutputFormat.class,
		    new Configuration()
		);

		// We can choose to update documents in an existing collection by using the
		// MongoUpdateWritable class instead of BSONObject. First, we have to create
		// the update operations we want to perform by mapping them across our current
		// RDD.
		JavaPairRDD<Object, MongoUpdateWritable> updates = documents.mapValues(
		    new Function<BSONObject, MongoUpdateWritable>() {
		        public MongoUpdateWritable call(BSONObject value) {
		            return new MongoUpdateWritable(
		                new BasicDBObject("_id", value.get("_id")),  // Query
		                new BasicDBObject("$set", new BasicDBObject("foo", "bar")),  // Update operation
		                false,  // Upsert
		                false   // Update multiple documents
		            );
		        }
		    }
		);

		// Now we call saveAsNewAPIHadoopFile, using MongoUpdateWritable as the value
		// class.
		updates.saveAsNewAPIHadoopFile(
		    "file://this-is-completely-unused",
		    Object.class,
		    MongoUpdateWritable.class,
		    MongoOutputFormat.class,
		    outputConfig
		);
	}

}
