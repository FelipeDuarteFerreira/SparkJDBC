import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Hello world! This program is for- transferring data from HDFS(json file) to
 * MongoDB
 */
public class HdfsMongo {

	@SuppressWarnings("deprecation")
	public static void main(final String[] args) throws InterruptedException {
		JavaSparkContext jsc = createJavaSparkContext(args);

		SQLContext sqlContext = new SQLContext(jsc);
		Dataset<Row> dataset = sqlContext.read().json(
				"hdfs://master:9000/" + "mongo2/emp2" + "/part-r-00000-126558e6-333c-4c24-970f-44407ade9d7d.json");

		MongoSpark.save(dataset);
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
			uri = "mongodb://localhost/test.fromhdfs"; // default
		} else {
			uri = args[0];
		}
		return uri;
	}

}
