package vn.wss.wordsearch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.cassandra.api.java.JavaCassandraSQLContext;
import org.hsqldb.Row;
import org.joda.time.DateTime;

import scala.Tuple2;
import tachyon.thrift.WorkerService.Processor.returnSpace;

import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;


public class UserIDViewNews {
	private static final Logger logger = LogManager.getLogger(UserIDViewNews.class);
	private static final String DEFAULT_DRIVER = "net.sourceforge.jtds.jdbc.Driver";//"oracle.jdbc.driver.OracleDriver";
    private static final String DEFAULT_URL = "";
    private static final String DEFAULT_USERNAME = "";
    private static final String DEFAULT_PASSWORD = "";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf(true)
		.setAppName("UserID View News")
		//.setMaster("local[4]")
		.set("spark.cassandra.connection.host", "10.0.0.11");
		
		JavaSparkContext sc = new 	JavaSparkContext(conf);
		String fromStr = WordSearch.dateToString(new DateTime(2015, 3, 1, 0,
				0, 0).toDate());
		String nowStr = WordSearch.dateToString(new DateTime(2015, 3, 31, 23,
				59, 59).toDate());
		CassandraJavaRDD<CassandraRow> rawData = javaFunctions(sc)
				.cassandraTable("tracking", "tracking").where(
						"year_month = ? ", 20150714
						);	
		char[] arrOperators = { ',', '^', '*', '/', '+', '-', '&', '=', '<', '>', '=', '%', '(', ')', '{', '}', ';','.' };
		String regex = "(" + new String(arrOperators).replaceAll("(.)", "\\\\$1|").replaceAll("\\|$", ")"); // escape every char with \ and turn into "OR"			
		
		JavaRDD<CassandraRow> containUri=rawData
				.filter(new Function<CassandraRow,Boolean>(){
					public Boolean call(CassandraRow row) throws Exception {
						String uri=row.getString("uri");
						if (uri==null)
						{
							return false;										
						}
						return uri.contains("http://websosanh.vn/s/");
			}
		})
		/*.mapToPair(new MapFunction<CassandraRow, String,Integer>(){
			@Override
			public Tuple2<String,Integer> call(CassandraRow row) throws Exception{
				String uri=row.getString("uri");
				String key=uri.sp
			}
			})*/
		;
		JavaRDD<String> words = containUri.flatMap(new FlatMapFunction<CassandraRow, String>() {
			  public Iterable<String> call(CassandraRow rows) throws Exception { 
				  String s = rows.getString("uri");
				  return Arrays.asList(s.split("uri=http://websosanh.vn/s/")[1].split(regex)[0]); }
			});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			  public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
			});
		JavaPairRDD<String, Integer> reduceByKey = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			  public Integer call(Integer a, Integer b) { return a + b; }
			});
		List<Tuple2<String,Integer>> results = reduceByKey.collect();
		
		String driver =  DEFAULT_DRIVER;
        String url =  DEFAULT_URL;
        String username = DEFAULT_USERNAME;
        String password =  DEFAULT_PASSWORD;
        Connection connection = null;        
        Date at= new Date(System.currentTimeMillis());
		try{
			connection = SqlDB.createConnection(driver, url, username, password);
			for(int i=0;i<results.size();i++)
			{
				String sqlUpdate =String.format("INSERT INTO History_Search(KeyName,KeyHash,TotalCount,DateLog,WebID) VALUES(?,?,?,?,?)");
				List parameters = Arrays.asList(results.get(i)._1,results.get(i)._1.hashCode(),results.get(i)._2,at,1);
				SqlDB.update(connection, sqlUpdate, parameters);
				connection.commit();
			}        	
		}
		catch(Exception e)
        {
        	SqlDB.rollback(connection);
            e.printStackTrace();	        	
        }					
	}
	public static String dateToString(Date date) {
		String res = "";
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		// 2015-2-15 23:59:59+0700
		res += c.get(Calendar.YEAR) + "-";
		res += (c.get(Calendar.MONTH) + 1) + "-" + c.get(Calendar.DAY_OF_MONTH);
		res += " " + c.get(Calendar.HOUR_OF_DAY) + ":";
		res += c.get(Calendar.MINUTE) + ":" + c.get(Calendar.SECOND);
		res += "+0700";
		return res;
	}

}
