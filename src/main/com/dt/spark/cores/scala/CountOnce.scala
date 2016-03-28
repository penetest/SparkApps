package com.dt.spark.cores.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
 * CountOnce
 * @author macal
 * Created by Administrator on 2016/1/27.
 */
object CountOnce {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("CountOnce").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("E:\\workspases\\data\\CountOnce.txt")
    val word = data.map(line => line.toInt)
    val result =word.reduce(_^_)
    println(result)
  }
}
