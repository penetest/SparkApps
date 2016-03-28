package com.dt.spark.sparkSql.scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用 反射机制，进行RDD和fram的转换，scala版本
  * Created by macal on 2016/3/16.
  */
object RddAndDataFramTransformationReflection {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RddAndDataFramTransformationReflection").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val person = sc.textFile("E:\\workspases\\data\\person.txt").map(_.split(",")).map(p => Person(p(0).trim.toInt,p(1),p(2).trim.toInt)).toDF()
    person.registerTempTable("persons")

    val bigDatas = sqlContext.sql("select * from persons where age > 7")

    bigDatas.map(_.getValuesMap[Any](List("id","name","age"))).collect().foreach(println)
  }
}

case class Person(id:Int,name:String,age:Int)
