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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * Hello world! First we load data from mongodb to hdfs as a JSON file (separate
 * program) This program is for- transferring data from HDFS(json file) to mysql
 */
public class SqlTransform {

	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PWD = "root";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/think";
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

		//String bfile = "hdfs://master:9000/mongo2/emp2/part-r-00000-126558e6-333c-4c24-970f-44407ade9d7d.json";
         String bfile = "hdfs://master:9000/transform2/part-00000";
         /*StructType schema = DataTypes
			.createStructType(new StructField[] {
					DataTypes.createStructField("ADDRESS", DataTypes.StringType, false),
					DataTypes.createStructField("CR", DataTypes.DateType, false),
					DataTypes.createStructField("DID", DataTypes.FloatType, false),
					DataTypes.createStructField("DOA", DataTypes.FloatType, false),
					DataTypes.createStructField("DOE", DataTypes.TimestampType, false),
					DataTypes.createStructField("DOJ", DataTypes.DateType, false),
					DataTypes.createStructField("GEN", DataTypes.StringType, false),
					DataTypes.createStructField("GRADE", DataTypes.StringType, false),
					DataTypes.createStructField("HRA", DataTypes.FloatType, false),
					DataTypes.createStructField("INTIME", DataTypes.TimestampType, false),
					DataTypes.createStructField("NAME", DataTypes.StringType, false),
					DataTypes.createStructField("PF", DataTypes.FloatType, false),
					DataTypes.createStructField("REG", DataTypes.FloatType, false),
					DataTypes.createStructField("TA", DataTypes.FloatType, false),
					DataTypes.createStructField("_id", DataTypes.FloatType, false) });*/
         Dataset<Row> dataset = sqlContext.read().json(bfile);
         
         
        //Dataset<Row> dataset = sqlContext.read().json(bfile);
        //Dataset<Row> dataset = sqlContext.read().schema(schema).json(bfile);
		
	
		//Dataset<String> all = entireDB.toJSON();
		
		List<Row> all1 = dataset.collectAsList();
		
		//Dataset<Row> dataset1 = sparkSession.createDataFrame(all1, schema);
		

		//dataset.show();
		//********************************************
		

		
		
		/*JavaRDD<Row> rowData = dataset.map(new Function<String, String[]>() {
			@Override
			public String[] call(String line) throws Exception {
				return line.split("\t");
			}
		}).map(new Function<String[], Row>() {
			@Override
			public Row call(String[] r) throws Exception {
				return RowFactory.create(Integer.parseInt(r[0]), r[1], r[2], r[3],
						new Timestamp(DatatypeConverter.parseDateTime(r[4]).getTimeInMillis()));
			}
		});
*/
		
		
		
		//***********************************************
		
		
		
		
		//dataset.printSchema();
        //System.out.println(dataset.columns());
		dataset.write().jdbc(MYSQL_CONNECTION_URL, "transformysql2", properties);

	}

}
