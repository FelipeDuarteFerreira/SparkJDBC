import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world! This program is for- transferring data from Mysql to HDFS(as
 * json file)
 */
public class FromMysqlToHdfs {

	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PWD = "root";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/test";
	private static final JavaSparkContext sc = new JavaSparkContext(
			new SparkConf().setAppName("Spark Example").setMaster("local[*]"));

	private static final SparkSession sparkSession = SparkSession.builder().master("local").appName("mysql")
			.getOrCreate();

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put("user", MYSQL_USERNAME);
		properties.put("password", MYSQL_PWD);

		/*
		 * sparkSession.conf().set("user", MYSQL_USERNAME);
		 * sparkSession.conf().set("password", MYSQL_PWD);
		 */

		String tbTables = "(select * from employees) as categoty";

		Dataset<Row> entireDB = sparkSession.read().jdbc(MYSQL_CONNECTION_URL, tbTables, properties);
		Dataset<String> all = entireDB.toJSON();
		List<String> all1 = all.collectAsList();

		sc.parallelize(all1).coalesce(1).saveAsTextFile("hdfs://master:9000/MysqlToHDFS1");
	}

}
