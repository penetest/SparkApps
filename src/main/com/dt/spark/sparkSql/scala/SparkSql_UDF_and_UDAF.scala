package com.dt.spark.sparkSql.scala

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 本节写的是sparksql的 用户自定义函数和用自定义聚合函数
  * Created by macal on 2016/4/2.
  */
object SparkSql_UDF_and_UDAF {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSql_UDF_and_UDAF").setMaster("local[4]")//本地运行模式，开辟四个线程
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val datas = Array("hadoop","spark","hadoop","hadoop","hadoop","spark","hadoop","hadoop","spark")

    val dataRdd = sc.parallelize(datas)
    val dataRddRow = dataRdd.map(row => Row(row))
    val structType = StructType(Array(StructField("word",StringType,true)))

    val bigdataDf = sqlContext.createDataFrame(dataRddRow,structType)
    bigdataDf.registerTempTable("bigdataTable")

    sqlContext.udf.register("wordsLength",(input:String) => input.length)

    sqlContext.sql("select word,wordsLength(word) lengt from bigdataTable ").show()

    sqlContext.udf.register("wordscount",new MyUDAF)

    sqlContext.sql("select word,wordsLength(word) size,wordscount(word) as count from bigdataTable group by word").show()

  }
}

class MyUDAF extends UserDefinedAggregateFunction{
  /**
    * 指定具体的输入类型
    *
    * @return
    */
  override def inputSchema: StructType = StructType(Array(StructField("word",StringType,true)))

  /**
    * 在进行聚合的时候，每当有新值进来，对分组后的聚合如何计算，本地的聚合操作，相当hadoop mapreduce计算模型中的combainer
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = buffer(0) = buffer.getAs[Int](0) + 1
  /**
    * 聚合时，所处理数据结果的类型
    * @return
    */
  override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType,true)))

  /**
    * 最后在分布式节点进行 local reduce完成后 需要进行全局级别的merge操作
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =  buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)

  /**
    * 在聚合前，每组数据初始化结果
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0

  /**
    * 确保一致性
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 返回udaf 最后的计算结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)

  /**
    * udaf返回结果的类型
    * @return
    */
  override def dataType: DataType = IntegerType
}
