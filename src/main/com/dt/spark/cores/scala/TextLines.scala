package com.dt.spark.cores.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
 * TopNbasic
 * @author macal
 * Created by Administrator on 2016/1/20.
 */
object TextLines {

  def main (args: Array[String]) {
      val conf = new SparkConf()
      conf.setAppName("text lines")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("")

    val lines = data.map(line => (line,1))

    val textlines = lines.reduceByKey(_+_)

    textlines.collect().foreach(par => println(par._1+":"+par._2))

  }
}
