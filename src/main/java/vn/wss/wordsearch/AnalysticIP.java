package vn.wss.wordsearch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.shell.Count;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.DateTime;

import scala.Tuple2;
import tachyon.thrift.WorkerService.Processor.returnSpace;

import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public class AnalysticIP {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length<2){
			System.err.println("-----------------------------Miss parameters,Plz enter --------------------------------");
			System.exit(1);			
		}
		char[] arrOperators = { ',', '^', '*', '/', '+', '-', '&', '=', '<', '>', '=', '%', '(', ')', '{', '}', ';','.' };
		String regex = "(" + new String(arrOperators).replaceAll("(.)", "\\\\$1|").replaceAll("\\|$", ")"); // escape every char with \ and turn into "OR"
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf(true)
		.setAppName("UserID View News")
		//.setMaster("local[4]")
		.set("spark.cassandra.connection.host", "10.0.0.11");
		
		JavaSparkContext sc = new 	JavaSparkContext(conf);
		CassandraJavaRDD<CassandraRow> rawData = javaFunctions(sc)
				.cassandraTable("tracking", "tracking").where(
						"year_month = ? ", args[0]
						);	
		JavaRDD<CassandraRow> resultByWebsiteId= rawData
				.filter( new Function<CassandraRow, Boolean>() {
					
					@Override
					public Boolean call(CassandraRow v1) throws Exception {
						// TODO Auto-generated method stub
						
						String websiteid =v1.getString("website_id");
						if (websiteid==null)
						{							
							return false;
						}
						else {
							return	websiteid.equals(args[1]);
						}												
					}
				}).cache();
		JavaPairRDD<String,Integer> result= resultByWebsiteId
				.mapToPair(new PairFunction<CassandraRow, String, Integer>() {
					@Override 
					public Tuple2<String, Integer> call(CassandraRow row) 
							throws Exception{						
						return new Tuple2<String, Integer>(row.getString("ip"), 1);
					}
				})
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
				})
				;
		for (Tuple2<String, Integer> item : result.collect())
		{
			System.out.println("----------------"+"ip:  "+item._1()+"-------------"+"count:  "+item._2());			
		}
	}
}
