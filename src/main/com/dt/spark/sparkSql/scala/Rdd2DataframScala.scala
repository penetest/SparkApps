package com.dt.spark.sparkSql.scala

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 使用编程指定模式 实现RDD和datafram的转换 scala版本
  * Created by macal on 2016/3/16.
  */
object Rdd2DataframScala {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Rdd2DataframScala").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val datas = sc.textFile("E:\\workspases\\data\\person.txt")

    val strschma = "id name age"

    val schema = StructType( strschma.split(" ").map(fieldname => StructField(fieldname,StringType,true)))

    val rowRdd = datas.map(_.split(",")).map(p => Row(p(0).trim,p(1),p(2).trim))

    val df = sqlContext.createDataFrame(rowRdd,schema)
    df.registerTempTable("persons")

    val bigdatas = sqlContext.sql("select * from persons where age >= 7")

    bigdatas.map(_.getValuesMap[Any](List("id","name","age"))).collect().foreach(println)
  }
}
