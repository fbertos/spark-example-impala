package org.fbertos.spark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class SparkDriverProgram {
    @SuppressWarnings("unchecked")
	public static void main(String args[]) throws Exception {
    	String driverName = "com.cloudera.impala.jdbc4.Driver";
    	 Class.forName(driverName);

    	 Connection con = DriverManager.getConnection("jdbc:impala://localhost:21050/test", "", "");
    	 Statement stmt = con.createStatement();
    	 
    	 
    	 String sql = "select * from employee";
    	 ResultSet res = stmt.executeQuery(sql);
    	 
    	 while (res.next()) {
    	    System.out.println(res.getString(1) + "\t" + res.getString(2));
    	 }
    }
}
