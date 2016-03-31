package com.dt.spark.sparkSql.scala

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._  //引入函数内部的函数

/**
  * Created by Administrator on 2016/3/29.
  */
object SparkSqlAgg {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sparksql innner function").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._  //增加隐式转换，一般使用sparksql  都需要用到的

    val userData = Array(
      "2016-3-28,002,www.baidu.com,1210",
      "2016-3-22,002,www.google.com,1850",
      "2016-3-26,003,www.jd.com,1270",
      "2016-3-24,001,www.ptopenlab.com,1260",
      "2016-3-24,001,www.apache.org,1550",
      "2016-3-28,002,www.baidu.com,1350");

    val userDataRdd = sc.parallelize(userData)

    val userDataRow = userDataRdd.map(row => {val spliteddata = row.split(",");Row(spliteddata(0),spliteddata(1).toInt,spliteddata(2),spliteddata(3).toInt)
    })

    val structTypes = StructType(Array(
      StructField("date",StringType,true),
      StructField("id",IntegerType,true),
      StructField("url",StringType,true),
      StructField("amount",IntegerType,true)
    ))

    val userDatadf = sqlContext.createDataFrame(userDataRow,structTypes)

    userDatadf.groupBy("date").agg(sum('amount) as '销售量).show

    userDatadf.select(userDatadf("date"),userDatadf("amount") as 'maxdd).orderBy('amount.desc).limit(1).show

    userDatadf.registerTempTable("user")

    sqlContext.sql("select sum(amount) from user where id = 001 and date = \'2016-3-24\'").show()
    //sqlContext.sql("select * from user where amount = ( select max(amount) from user )").show()
    //userDatadf.groupBy("amount").agg('date,max('amount)).show


  }
}
