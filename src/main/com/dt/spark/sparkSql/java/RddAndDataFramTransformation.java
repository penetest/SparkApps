package com.dt.spark.sparkSql.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


/**
 *  使用反射机制， RDD和datafram的相互转化  java版本
 * Created by macal on 2016/3/16.
 */
public class RddAndDataFramTransformation {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RddAndDataFramTransformation").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext =  new SQLContext(sc);

        JavaRDD<String> datas = sc.textFile("E:\\workspases\\data\\person.txt");

        JavaRDD<Person> persons = datas.map(new Function<String, Person>() {
            @Override
            public Person call(String line) throws Exception {
                String[] splitedline = line.split(",");
                Person p = new Person();
                p.setAge(Integer.valueOf(splitedline[2]));
                p.setId(Integer.valueOf(splitedline[0]));
                p.setName(splitedline[1]);
                return p;
            }
        });

        DataFrame schemaPerson = sqlContext.createDataFrame(persons,Person.class);
        schemaPerson.registerTempTable("persons");

        DataFrame bigDatas = sqlContext.sql("select * from persons where age >= 7");
        JavaRDD<Row>  bigDataRDD = bigDatas.javaRDD();

        JavaRDD<Person> result = bigDataRDD.map(new Function<Row, Person>() {
            @Override
            public Person call(Row row) throws Exception {
                Person p = new Person();
                //这个地方会出现类型转换问题  ，因为datafram在计算的时候会出现排序
                p.setId(row.getInt(1));
                p.setName(row.getString(2));
                p.setAge(row.getInt(0));
                return p;
            }
        });

        result.foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                System.out.println(person.toString());
            }
        });

        sc.stop();
    }
}

