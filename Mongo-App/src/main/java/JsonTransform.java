import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;

public class JsonTransform {
	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PWD = "root";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/think";
	private static final JavaSparkContext sc = new JavaSparkContext(
			new SparkConf().setAppName("Spark Example").setMaster("local[*]"));
    @SuppressWarnings("deprecation")
	private static final SQLContext sqlContext = new SQLContext(sc);

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put("user", MYSQL_USERNAME);
		properties.put("password", MYSQL_PWD);
		String bfile = "hdfs://master:9000/transform2/part-00000";
		//String bfile = "hdfs://ubuntu:9000/Bio/part-00000";
		Dataset<Row> ds = sqlContext.read().json(bfile);
		ds.show();
		ds.printSchema();
		
		StructType schema = (new StructType())
				.add("ADDRESS", DataTypes.StringType)
				.add("DID", DataTypes.IntegerType)
				.add("CR", DataTypes.DateType)
		        .add("DOA", DataTypes.TimestampType)
				.add("DOE", DataTypes.TimestampType)
				.add("DOJ", DataTypes.DateType)
				.add("GEN", DataTypes.StringType)
				.add("GRADE", DataTypes.StringType)
				.add("HRA", DataTypes.FloatType)
				.add("INTIME", DataTypes.TimestampType)
				.add("NAME", DataTypes.StringType)
				.add("PF", DataTypes.FloatType)
				.add("REG", DataTypes.LongType)
				.add("TA", DataTypes.DoubleType)
				.add("_id", DataTypes.IntegerType);
		Dataset<Row> ds1 = sqlContext.read().schema(schema).json(bfile);
		ds1.printSchema();
		ds1.show();
		//ds1.write().jdbc(MYSQL_CONNECTION_URL, "transformysqltest2", properties);
	}
}