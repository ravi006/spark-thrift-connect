package com.ravi.spark.thrift.connect.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by ravi on 11/8/16.
 */
public class ConnectThrift {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String SPARK_THRIFT_SERVER_URL = "jdbc:hive2://ec2-321-44-578.compute-1.amazonaws.com:10001/ade";

    private static String DB_USER = "hadoop";
    private static String DB_PASSWORD = "";

    public static Connection getThriftConnection(){
        Connection con = null;
        try {
            Class.forName(driverName);
             con = DriverManager.getConnection(SPARK_THRIFT_SERVER_URL, DB_USER, DB_PASSWORD);

        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return con;
    }
}
