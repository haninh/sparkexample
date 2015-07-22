package vn.wss.wordsearch;

import java.sql.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class CassandraDB {
	private static final Logger log = LogManager.getLogger(CassandraDB.class);
	private static CassandraDB instance = new CassandraDB();
	static Object syncRoot = new Object();
	Cluster cluster;
	Session session;
	final String CONTACT_POINT_1 = "10.0.0.11";
	final String CONTACT_POINT_2 = "10.0.0.12";
	final String CONTACT_POINT_3 = "10.0.0.13";
	final String CONTACT_POINT_4 = "10.0.0.14";
	final String KEYSPACE = "tracking";

	PreparedStatement insertTracking;

	public static CassandraDB getInstance() {
//		if (instance == null) {
//			synchronized (syncRoot) {
//				if (instance == null) {
//					instance = new CassandraDB();
//				}
//			}
//		}
		return instance;
	}

	private CassandraDB() {
		log.info("create new instance");
		connect();
		createTrackingTable();
		prepareStatement();
	}

	void connect() {
		cluster = Cluster.builder().addContactPoint(CONTACT_POINT_1)
				.addContactPoint(CONTACT_POINT_2).addContactPoint(CONTACT_POINT_3)
				.addContactPoint(CONTACT_POINT_4).build();
		//log.info("Connected to cluster " + cluster.getClusterName());
		log.info("Hosts: " + cluster.getMetadata().getAllHosts());
		session = cluster.connect(KEYSPACE);
	}

	Session getSession() {
		if (session == null) {
			connect();
		}
		return session;
	}

	void close() {
		log.info("close connection");
		session.close();
		cluster.close();
	}

	void createTrackingTable() {
		// String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS tracking"
		// +
		// "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};";
		// session.execute(createKeyspace);

		String createTableTracking = "CREATE TABLE IF NOT EXISTS tracking ("
				+ "year_month int," + "at timestamp," + "session_id text,"
				+ "user_id text," + "uri text," + "referer text," + "ip text,"+"website_id int,"
				+ "PRIMARY KEY (year_month, at)" + ");";
		session.execute(createTableTracking);
	}

	void prepareStatement() {
		insertTracking = session
				.prepare("INSERT INTO tracking.tracking"
						+ "(year_month, at, session_id, user_id, uri, referer, ip, website_id)"
						+ "VALUES    (?         , ? , ?         , ?      , ?  , ?      , ? ,?)");
	}

	public void insertTracking(Date at, String sessionId, String userId,
			String uri, String referer, String ip,Integer websiteId) {
		int year = at.getYear() + 1900;
		int month = at.getMonth() + 1;
		int date=at.getDate();
		int yearMonth = year * 10000 + month*100+date;
		BoundStatement statement = insertTracking.bind(yearMonth, at,
				sessionId, userId, uri, referer, ip, websiteId);
		session.execute(statement);
	}
}