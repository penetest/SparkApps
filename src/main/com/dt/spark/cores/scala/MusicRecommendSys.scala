package com.dt.spark.cores.scala

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * music recommend system
  * make by macal
  * Created by macal on 2016/3/3.
  */
object MusicRecommendSys {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("Music Recommend System")
    val sc = new SparkContext(conf)

    val basepath = "/data/profiledata_06-May-2005/" //设置HDFS的基本路径
    val rawArtistAlias = sc.textFile(basepath+"artist_alias.txt",32)
    val rawArtistData  = sc.textFile(basepath+"artist_data.txt",32)
    val rawUserArtistData = sc.textFile(basepath+"user_artist_data.txt",32)

    //准备数据
    //PreparateDatas(rawArtistAlias,rawArtistData,rawUserArtistData)
    //创建数据模型
    //model(sc,rawArtistAlias,rawArtistData,rawUserArtistData)
    //评估模型
    //evaluate(sc,rawUserArtistData  ,rawArtistAlias )
    //推荐music
    recommend(sc,rawUserArtistData ,rawArtistData ,rawArtistAlias)
    sc.stop()
  }

  /**
    * 处理艺术家数据
    *
    * @param rawArtistData
    * @return
    */
  def buildArtistByID(rawArtistData:RDD[String])=
    rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }

  /**
    * 艺术家的标准名称
    *
    * @param rawArtistAlias
    * @return
    */
  def buildArtistAlias(rawArtistAlias: RDD[String]): Map[Int, Int] =
    rawArtistAlias.flatMap{line =>
      val tokens = line.split('\t')
      if(tokens(0).isEmpty){
        None
      }else{
        Some((tokens(0).toInt,tokens(1).toInt))
      }
    }.collectAsMap()

  /**
    * 准备数据
    *
    * @param rawArtistAlias
    * @param rawArtistData
    * @param rawUserArtistData
    */
  def PreparateDatas(rawArtistAlias:RDD[String],
                     rawArtistData:RDD[String],
                     rawUserArtistData:RDD[String])={
    val userIDStats = rawUserArtistData.map(_.split(" ")(0).toDouble).stats()
    val itemIDStats = rawUserArtistData.map(_.split(" ")(1).toDouble).stats()
    println("userID的状况:"+userIDStats)
    println("artistID的状况:"+itemIDStats)

    val artistByID = buildArtistByID(rawArtistData)
    val artistAlias = buildArtistAlias(rawArtistAlias)

    val (badID,goodID) = artistAlias.head
    println(artistByID.lookup(badID)+"->"+artistByID.lookup(goodID))
  }

  def buildRatings(
                    rawUserArtistData:RDD[String],
                    bArtistAlias:Broadcast[Map[Int,Int]]) = {
    rawUserArtistData.map{line =>
      val Array(userID,artistID,count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID,artistID)
      Rating(userID,finalArtistID,count)
    }
  }

  /**
    * 创建推荐模型
    *
    * @param sc
    * @param rawArtistAlias
    * @param rawArtistData
    * @param rawUserArtistData
    */
  def model(
             sc: SparkContext,
             rawArtistAlias:RDD[String],
             rawArtistData:RDD[String],
             rawUserArtistData:RDD[String]):Unit ={
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))//创建广播变量
    val trainData = buildRatings(rawUserArtistData,bArtistAlias ).cache()

    val model = ALS.trainImplicit(trainData,10,5,0.01,1.0)

    trainData.unpersist()

    println(model.userFeatures.mapValues(_.mkString(", ")).first)

    val userID = 2093760
    val recommendations = model.recommendProducts(userID,5)
    recommendations.foreach(println)

    val recommendedProductIDs = recommendations.map(_.product).toSet

    val rawArtistsForUser = rawUserArtistData.map(_.split(" ")).filter{case Array(user,_,_) => user.toInt == userID}

    val existingProducts = rawArtistsForUser.map{ case Array(_,artist,_) => artist.toInt}.collect().toSet

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter{ case(id,name) => existingProducts.contains(id)}.values.collect().foreach(println)
    artistByID.filter{ case (id,name) => recommendedProductIDs.contains(id)}.values.collect().foreach(println)

    unpersist(model)
  }

  /**
    * 释放内存
    *
    * @param model
    */
  def unpersist(model:MatrixFactorizationModel):Unit ={

    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }

  /**
    * 评估模型
    *
    * @param sc
    * @param rawUserArtistData
    * @param rawArtistAlias
    */
  def evaluate(
                sc:SparkContext,
                rawUserArtistData:RDD[String],
                rawArtistAlias:RDD[String]):Unit = {
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val alldata = buildRatings(rawUserArtistData,bArtistAlias)
    val Array(trainData,cvData) = alldata.randomSplit(Array(0.9,0.1))
    trainData.cache()
    cvData.cache()

    val allItemIDs = alldata.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)

    val mostListenedAUC = areaUnderCurve(cvData,bAllItemIDs,predictMostListened(sc,trainData))

    println(mostListenedAUC)

    val evaluations =
      for( rank <- Array(10,50);
           lambda <- Array(1.0,0.0001);
           alpha <- Array(1.0,40.0))
        yield {
          val model = ALS.trainImplicit(trainData,rank,10,lambda ,alpha)
          val auc = areaUnderCurve(cvData,bAllItemIDs,model.predict)
          unpersist(model)
          ((rank,lambda,alpha),auc)
        }
    evaluations.sortBy(_._2).reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()
  }

  /**
    * 计算AUC
    *
    * @param positiveData
    * @param bAllItemIDs
    * @param predictFunction
    */
  def areaUnderCurve(
                      positiveData:RDD[Rating],
                      bAllItemIDs:Broadcast[Array[Int]],
                      predictFunction:(RDD[(Int,Int)] => RDD[Rating])) = {
    val positiveUserProducts = positiveData.map(r => (r.user,r.product))
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions{

      userIdAndPostItemIDs => {
        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIdAndPostItemIDs.map{case(userID,postItemIDs) =>
          val posItemIDSet = postItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0

          while (i < allItemIDs.size && negative.size < posItemIDSet.size){
            val itemId = allItemIDs(random.nextInt(allItemIDs.size))
            if(!posItemIDSet.contains(itemId)){
              negative += itemId
            }
            i += 1
          }
          negative.map(itemID => (userID,itemID))

        }
      }
    }.flatMap(t => t)

    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    positivePredictions.join(negativePredictions).values.map{
      case(positiveRating,negativeRatings) =>

        var correct = 0L
        var total = 0L

        for(positive <-positiveRating; negative <- negativeRatings){
          if(positive.rating > negative.rating){
            correct += 1
          }
          total += 1
        }
        correct.toDouble / total
    }.mean()

  }

  def predictMostListened(sc:SparkContext, train:RDD[Rating])(allData: RDD[(Int,Int)]) = {
    val bListenCount =
      sc.broadcast(train.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap())
    allData.map { case (user, product) =>
      Rating(user, product, bListenCount.value.getOrElse(product, 0.0))
    }
  }

  def recommend(
                 sc:SparkContext,
                 rawUserArtistData:RDD[String],
                 rawArtistData:RDD[String],
                 rawArtistAlias:RDD[String]):Unit = {

    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    val allData = buildRatings(rawUserArtistData,bArtistAlias).cache()
    val model = ALS.trainImplicit(allData,50,10,1.0,40.0)
    allData.cache()

    val userID = 2093760
    val recommendations = model.recommendProducts(userID,5)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter{ case(id,name) => recommendedProductIDs.contains(id)}.values.collect().foreach(println)

    val someUsers = allData.map(_.user).distinct().take(100)
    val someRecommendations = someUsers.map(userID => model.recommendProducts(userID,5))
    someRecommendations.map(
      recs => recs.head.user + "->" + recs.map(_.product).mkString(", ")
    ).foreach(println)

    unpersist(model)
  }
}
