package com.dt.spark.cores.scala

import org.apache.spark.{SparkContext, SparkConf}


/**
 *  求中位数
 * @author macal
 * Created by Administrator on 2016/1/26.
 */
object Median {

  def main (args: Array[String]) {

    val conf = new SparkConf().setAppName("Median").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("E:\\workspases\\data\\Median.txt")

    val words = data.flatMap(_.split(" ")).map(word => word.toInt)
    val number = words.map(word =>(word/4,word)).sortByKey()
    val pariCount = words.map(word => (word/4,1)).reduceByKey(_+_).sortByKey()
    val count = words.count().toInt
    var mid =0
    if(count%2 != 0)
      {
        mid = count/2+1
      }else
      {
        mid = count/2
      }

    var temp =0
    var temp1= 0
    var index = 0
    val tongNumber = pariCount.count().toInt

    var foundIt  = false
    for(i <- 0 to tongNumber-1 if !foundIt)
    {
      temp = temp + pariCount.collectAsMap()(i)
      temp1 = temp - pariCount.collectAsMap()(i)
      if(temp >= mid)
        {
          index = i
          foundIt  = true
        }
    }
    val tonginneroffset = mid - temp1

    val median = number.filter(_._1==index).takeOrdered(tonginneroffset)
    sc.setLogLevel("ERROR")
    println(median(tonginneroffset-1)._2)
    sc.stop()

  }
}
