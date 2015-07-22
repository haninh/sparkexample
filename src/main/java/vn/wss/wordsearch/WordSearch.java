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

public class WordSearch {
	private static final Logger logger = LogManager.getLogger(WordSearch.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true).set(
				"spark.cassandra.connection.host", "10.0.0.11");

		JavaSparkContext sc = new JavaSparkContext(conf);
		String fromStr = WordSearch.dateToString(new DateTime(2015, 3, 1, 0,
				0, 0).toDate());
		String nowStr = WordSearch.dateToString(new DateTime(2015, 3, 31, 23,
				59, 59).toDate());
		CassandraJavaRDD<CassandraRow> rawData = javaFunctions(sc)
				.cassandraTable("tracking", "tracking").where(
						"year_month = ? AND at > ? and at < ?", 201503,
						fromStr, nowStr);
//		JavaRDD<String> lazada = sc.textFile("/lazada.txt");
		// String url =
		// "http://websosanh.vn/gionee-gn800-5-5mp-2gb-2-sim-trang/3120163158968901227/direct.htm";
//		System.out.println(rawData.count());
//		JavaPairRDD<String, Integer> lazadaID = lazada
//				.mapToPair(new PairFunction<String, String, Integer>() {
//
//					@Override
//					public Tuple2<String, Integer> call(String t)
//							throws Exception {
//						// TODO Auto-generated method stub
//						return new Tuple2<String, Integer>(t, 1);
//					}
//				});
		long count = rawData
				.filter(new Function<CassandraRow, Boolean>() {

					@Override
					public Boolean call(CassandraRow v1) throws Exception {
						// TODO Auto-generated method stub
						String uri = v1.getString("uri");
//						logger.info(uri);
						if (uri == null) {
							return false;
						}
						return uri.equals("http://websosanh.vn/may-dieu-hoa-de-ban-mini-loai-1-usa-store-1081-xanh/1976639316872890076/direct.htm");
					}
				}).count();
//				.mapToPair(new PairFunction<CassandraRow, String, Integer>() {
//
//					@Override
//					public Tuple2<String, Integer> call(CassandraRow t)
//							throws Exception {
//						// TODO Auto-generated method stub
//						String[] s = t.getString("uri").split("/");
//						String key = s[s.length - 2];
//						return new Tuple2<String, Integer>(key, 1);
//					}
//				})
//				.reduceByKey(new Function2<Integer, Integer, Integer>() {
//
//					@Override
//					public Integer call(Integer v1, Integer v2)
//							throws Exception {
//						// TODO Auto-generated method stub
//						return v1 + v2;
//					}
//				})
//				.join(lazadaID)
//				.filter(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Boolean>() {
//
//					@Override
//					public Boolean call(
//							Tuple2<String, Tuple2<Integer, Integer>> v1)
//							throws Exception {
//						// TODO Auto-generated method stub
//						return v1._2()._2() == 1;
//					}
//				}).count();
		logger.info("Lazada click: " + count);
		System.out.println("------------------------Ta La Sieu Nhan----------------------");
		sc.stop();
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
