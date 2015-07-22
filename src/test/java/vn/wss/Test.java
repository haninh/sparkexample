package vn.wss;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;

import vn.wss.wordsearch.*;

public class Test {
		private static final String DEFAULT_DRIVER = "net.sourceforge.jtds.jdbc.Driver";//"oracle.jdbc.driver.OracleDriver";
	    private static final String DEFAULT_URL = "jdbc:jtds:sqlserver://192.168.100.169/Ninh_log";
	    private static final String DEFAULT_USERNAME = "thudt";
	    private static final String DEFAULT_PASSWORD = "12qwAS";
	@org.junit.Test
	public void test() {
		String s ="http://websosanh.vn";
		//System.out.println(StringUtils.getWordSearch(s));
		//SqlDB.getInstance().insertKeyword(20150710, "thuxinhgai", 2015000, 10);
		System.out.println("Ta la sieu nhan!!!!");
		String fromStr = WordSearch.dateToString(new DateTime(2015, 3, 1, 0,
				0, 0).toDate());
		System.out.println(fromStr);
		//SqlDB.getInstance().printString("a");
	/*	long begTime = System.currentTimeMillis();
		 	String driver =  DEFAULT_DRIVER;
	        String url =  DEFAULT_URL;
	        String username = DEFAULT_USERNAME;
	        String password =  DEFAULT_PASSWORD;
	        Connection connection = null;
	        try
	        {
	        	connection = SqlDB.createConnection(driver, url, username, password);
	        	String sqlUpdate =String.format("INSERT INTO Keyword(Year_Month_Date,Keyword,HashValue,Count) VALUES(?,?,?,?)",20150711,6363663,4111515,1414);
	            //String sqlUpdate =String.format("UPDATE Keyword SET Year_Month_Date=?, Keyword=?, HashValue=?, Count=? WHERE ID=?");
	            List parameters = Arrays.asList(20150711,"----------Ta la Sieu Nhan---------",4111515,1414);
	            int numRowsUpdated =SqlDB.update(connection, sqlUpdate, parameters);
	            //connection.commit();
	            System.out.println("# rows inserted: " + numRowsUpdated);
	        }
	        catch(Exception e)
	        {
	        	SqlDB.rollback(connection);
	            e.printStackTrace();	        	
	        }	        
	 */
		
	}

}
