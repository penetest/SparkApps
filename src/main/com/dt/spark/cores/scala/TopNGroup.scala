package com.dt.spark.cores.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
 * *使用java的方式开发分组topN程序
 * @author macal
 * Created by Administrator on 2016/1/25.
 */
object TopNGroup {
  def main (args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("TopNBasic")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("E:\\workspases\\data\\GroupTopN.txt")

    val groupRDD = data.map(line => (line.split(" ")(0),line.split(" ")(1).toInt)).groupByKey()

    val top5 = groupRDD.map(pair=> (pair._1,pair._2.toList.sortWith(_>_).take(5))).sortByKey()
    top5.collect().foreach(pair =>{
      println(pair._1+":")
      pair._2.foreach(println)
      println("***********************")
    })

  }
}
