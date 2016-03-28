package com.dt.spark.cores.scala

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable


/**
 * 倒排序 spark实例
 * @author macal
 * Created by Administrator on 2016/1/26.
 */
object InvertedIndex {
  def main (args: Array[String]) {

    val conf = new SparkConf().setAppName("inverted index").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("E:\\workspases\\data\\invertedIndex.txt")
    val pairs = data.map(line => line.split("\t")).map(item => (item(0),item(1)))
    val inverting = pairs.flatMap(file=>{
      val map = new mutable.LinkedHashMap[String,String]()
      val words = file._2.split(" ").iterator
      while(words.hasNext)
        {
          map.put(words.next(),file._1)
        }
      map
    })

    inverting.reduceByKey(_+" "+_).map(file => file._1+"\t"+file._2).foreach(println)
    sc.stop()
  }
}
