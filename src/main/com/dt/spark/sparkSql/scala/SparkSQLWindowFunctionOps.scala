package com.dt.spark.sparkSql.scala

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sparksql窗口函数的操作
  * Created by macal on 2016/4/2.
  */
object SparkSQLWindowFunctionOps {
  def main(args: Array[String]) {
    val conf  = new SparkConf().setAppName("SparkSQLWindowFunctionOps")
    val sc  = new SparkContext(conf)

    val sqlContext  = new HiveContext(sc)

    sqlContext.sql("use hive")
    sqlContext.sql("drop table if exists scores")
    sqlContext.sql("create table if not exists  scores(name STRING,score INT) Row format delimited fields terminated by ' ' lines terminated by '\\n'")
    sqlContext.sql("load data local inpath  '/home/opuser/datas/scores.txt' into table scores")

    val result = sqlContext.sql("select name,score " +
      "from ( " +
      "select " +
      "name," +
      "score," +
      "row_number() over ( partition by name order by score desc) rank " +
      "from scores )" +
      " sub_scores " +
      " where rank <= 4")
    result.show()
    sqlContext.sql("drop table if exists scoreselectresult")

    result.saveAsTable("scoreselectresult")

    sc.stop()


  }
}
