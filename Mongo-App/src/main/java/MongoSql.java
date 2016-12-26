import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world! First we load data from mongodb to hdfs as a JSON file (separate
 * program) This program is for- transferring data from HDFS(json file) to mysql
 */
public class MongoSql {

	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PWD = "root";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/test";
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
		 * sparkSession.conf().set("user", "root");
		 * sparkSession.conf().set("password", "root");
		 */

		String bfile = "hdfs://master:9000/mongo2/emp2/part-r-00000-126558e6-333c-4c24-970f-44407ade9d7d.json";

		Dataset<Row> dataset = sqlContext.read().json(bfile);

		dataset.write().jdbc(MYSQL_CONNECTION_URL, "mongotab1", properties);

	}

}
