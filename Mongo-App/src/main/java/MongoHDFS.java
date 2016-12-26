import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import org.bson.BSONObject;
import org.bson.Document;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Hello world! This program is for- transferring data from MongoDB's tables to
 * HDFS( as JSON).
 */
public class MongoHDFS {

	@SuppressWarnings("deprecation")
	public static void main(final String[] args) throws InterruptedException {
		JavaSparkContext jsc = createJavaSparkContext(args);

		SQLContext sqlContext = new SQLContext(jsc);

		Dataset<Row> df = MongoSpark.load(jsc).toDF();

		df.write().json("hdfs://master:9000/mongo2/emp2");
	}

	private static JavaSparkContext createJavaSparkContext(final String[] args) {
		String uri = getMongoClientURI(args);

		SparkConf conf = new SparkConf().setMaster("local").setAppName("MongoSparkConnectorTour")
				.set("spark.app.id", "MongoSparkConnectorTour").set("spark.mongodb.input.uri", uri)
				.set("spark.mongodb.output.uri", uri);

		return new JavaSparkContext(conf);
	}

	private static String getMongoClientURI(final String[] args) {
		String uri;
		if (args.length == 0) {
			uri = "mongodb://localhost/test.users";
		} else {
			uri = args[0];
		}
		return uri;
	}

}
