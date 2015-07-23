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
public class GetByUserId {

	public static void main(String[] args) {
		if(args.length<1){
			System.err.println("Miss UserId,Plz enter UserId");
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
						"year_month = ? ", 20150714
						);	
		JavaRDD<CassandraRow> reduceByKey =rawData
				.filter(new Function<CassandraRow,Boolean>(){
					public Boolean call(CassandraRow row) throws Exception {
						String uri=row.getString("user_id");
						if (uri==null)
						{
							return false;										
						}
						return uri.contains(args[0]);
			}
		});
		/*.mapToPair(new PairFunction<CassandraRow, String, Integer>() {
			//
			@Override
			public Tuple2<String, Integer> call(CassandraRow row)
				throws Exception {
			// TODO Auto-generated method stub
				String uri=row.getString("uri");
				String key=uri.split("uri=http://websosanh.vn/s/")[1].split(regex)[0];
				return new Tuple2<String, Integer>(key, 1);
			}
		})
		.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				});*/
		//.count();
		
		System.out.println("------------------------------------result : " + reduceByKey.count()+"--------------------------------------");
		//List<Tuple2<String,Integer>> results = reduceByKey.collect();
		if(reduceByKey.count()>0)
		{
			for(CassandraRow row : reduceByKey.collect())
			{				
				System.out.println("year_month: "+ row.getString("year_month")+"--------"+"uri: "+row.getString("uri"));
			}						
		}
		else {
			System.out.println("-----------no result!!!------------");
		}
		
	}

}
