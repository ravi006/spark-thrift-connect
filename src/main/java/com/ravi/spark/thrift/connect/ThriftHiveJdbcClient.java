package com.ravi.spark.thrift.connect;

import com.ravi.spark.thrift.connect.utils.ConnectThrift;

import java.sql.*;

/**
 * Created by ravi on 11/8/16.
 */

public class ThriftHiveJdbcClient {

    /**
     *
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {

        /**
         * Jar usage
         * mvn package
         * java -cp /path/thrift-HiveConnect-0.0.1-SNAPSHOT.jar com.amp.hivejdbc.ThriftHiveJdbcClient
                private static String driverName = "org.apache.hive.jdbc.HiveDriver";
         */

        Connection con = ConnectThrift.getThriftConnection();
        Statement stmt = con.createStatement();
        String tableName = "d_table_2016";
        //use ade
        String dbsql = "use db_name";
        System.out.println("Running: " + dbsql);
        ResultSet resdb = stmt.executeQuery(dbsql);


        // show tables
        String sql = "show tables ";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);

        while (res.next()) {
            System.out.println(res.getString("tableName"));
        }
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }


        sql = "select count(*) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

    }
    }

