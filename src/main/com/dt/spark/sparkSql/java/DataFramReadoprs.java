package com.dt.spark.sparkSql.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * datafram 的读和写机制
 * Created by macal on 2016/3/17.
 */
public class DataFramReadoprs {
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("DataFramReadoprs").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        //DataFrame df = sqlContext.read().format("json").load("E:\\workspases\\data\\resources\\people.json");

       // DataFrame df = sqlContext.sql("select * from parquet.'E:\\workspases\\data\\resources\\users.parquet'");
        //df.show();
        //df.select("age").write().mode(SaveMode.Append).save("E:\\workspases\\data\\resources\\namesAndFavColors.parquet");

        sc.stop();
    }
}
