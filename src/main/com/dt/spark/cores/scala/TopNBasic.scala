package com.dt.spark.cores.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
 * @author macal
 * Created by Administrator on 2016/1/25.
 */
object TopNBasic {
  def main (args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("TopNBasic")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("E:\\workspases\\data\\basicTopN.txt")

    val pairs = data.map(line => (line.toInt,line))

    val sortedPairs = pairs.sortByKey(false)

    val sortedData = sortedPairs.map(pair => pair._2)

    val top5 = sortedData.take(5)

    sc.setLogLevel("OFF")
    top5.foreach(println)

    sc.stop()
  }
}
