
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * 
 */

/**
 * Hello world! This program is for- transferring data from HDFS(json file) to
 * Amazon RedShift
 */
public class AwsRedshift {

	private static final String MYSQL_USERNAME = "admin"; // Redshift username
	private static final String MYSQL_PWD = "Admin123"; // Redshift password
	// get end point from amazon red shift dashboard and put in below line .
	private static final String MYSQL_CONNECTION_URL = "jdbc:redshift://redshift.cx7rlycokw7k.us-east-1.redshift.amazonaws.com:5439/cloudcdc";
	private static final JavaSparkContext sc = new JavaSparkContext(
			new SparkConf().setAppName("Spark Example").setMaster("local[*]"));

	private static final SparkSession sparkSession = SparkSession.builder().master("local").appName("mysql")
			.getOrCreate();
	private static final SQLContext sqlContext = new SQLContext(sc);

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put("user", MYSQL_USERNAME);
		properties.put("password", MYSQL_PWD);

		/*
		 * sparkSession.conf().set("user", "admin");
		 * sparkSession.conf().set("password", "Admin123");
		 */
		String[] tables = { "employees", "person" };
		for (String tableName : tables) {

			Dataset<Row> dataset = sqlContext.read().json("hdfs://master:9000/" + tableName + "/part-00000");

			dataset.write().jdbc(MYSQL_CONNECTION_URL, tableName, properties);

		}

	}

}
