package com.dt.spark.sparkSql.java;


import java.sql.*;

/**
 * 使用sparkSql Jdbc 连接ThriftServer
 * Created by macal on 2016/4/2.
 */
public class SparkSQLJDBC2ThriftServer {

    public static void main(String[] args) throws SQLException {
        String sql = "select *  from sogouq where rank > ?";
        Connection conn = null;
        ResultSet resultSet = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://nova-35e85a00-e15b-44c2-9634-e6397f25d92b:10001/hive?" +
                    "hive.server2.transport.mode=http;hive.server2.thrift.http.path=", "root", "");
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setInt(1,30);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next())
            {
                System.out.println(resultSet.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            resultSet.close();
            conn.close();
        }


    }
}