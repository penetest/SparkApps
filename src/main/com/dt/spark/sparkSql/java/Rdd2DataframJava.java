package com.dt.spark.sparkSql.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 使用编程指定模式 实现RDD和datafram的转换   java版本
 * Created by macal on 2016/3/16.
 */
public class Rdd2DataframJava {

    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("Rdd2DataframJava").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> datas = sc.textFile("E:\\workspases\\data\\person.txt");

        String strschem = "id name age";

        List<StructField> fields = new ArrayList<StructField>();

        for(String fieldName : strschem.split(" "))
        {
            fields.add(DataTypes.createStructField(fieldName,DataTypes.StringType,true));
        }

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = datas.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] fields = line.split(",");
                return RowFactory.create(fields[0].trim(),fields[1],fields[2].trim());
            }
        });

        DataFrame dataFrame = sqlContext.createDataFrame(rowJavaRDD,schema);

        dataFrame.registerTempTable("persons");

        DataFrame bigdatas = sqlContext.sql("select * from persons where age >= 7");

        JavaRDD<Row> bigdata = bigdatas.javaRDD();

        List<String> result =  bigdata.map(new Function<Row, String>() {
            public String call(Row row) {
                return "[ id: "+row.getString(0)+", Name: " + row.getString(1)+", age: "+row.getString(2)+"]";
            }
        }).collect();

        for (String s : result) {
            System.out.println(s);
        }
    }
}
