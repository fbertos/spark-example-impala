package org.fbertos.spark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;


public class SparkDriverProgram {
    @SuppressWarnings("unchecked")
	public static void main(String args[]) throws Exception {
    	 String driverName = "org.apache.hive.jdbc.HiveDriver";
    	 Class.forName(driverName);

    	 Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default;hive.server2.proxy.user=APP", "APP", "mine");
    	 Statement stmt = con.createStatement();
    	 
    	 
    	 String sql = "select * from employee";
    	 ResultSet res = stmt.executeQuery(sql);
    	 
    	 if (res.next()) {
    	    System.out.println(res.getString(1));
    	 }
    }
}
